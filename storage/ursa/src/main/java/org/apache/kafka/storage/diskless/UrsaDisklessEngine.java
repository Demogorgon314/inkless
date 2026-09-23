/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.storage.diskless;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.FileRecords;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.storage.diskless.handlers.UrsaDisklessTopicLifecycle;
import org.apache.kafka.storage.diskless.handlers.UrsaStorageConfig;
import org.apache.kafka.storage.diskless.handlers.UrsaStorageEngineImpl;
import org.apache.kafka.storage.internals.log.OffsetResultHolder.FileRecordsOrError;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import io.aiven.inkless.engine.DisklessEngine;
import io.aiven.inkless.engine.DisklessTopicLifecycle;

/** Bridges the broker SPI to the storage implementation copied from UFK. */
public final class UrsaDisklessEngine implements DisklessEngine {
    private UrsaStorageConfig config;
    private Context context;
    private DisklessStorageEngine storage;
    private DisklessTopicLifecycle lifecycle;

    @Override
    public void configure(Map<String, ?> configs) {
        try {
            config = UrsaStorageConfig.fromConfigs(configs);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid Ursa engine configuration", e);
        }
    }

    @Override
    public void initialize(Context context) {
        this.context = context;
        storage = new UrsaStorageEngineImpl(context.time(), context.brokerId(), config,
            context.metrics(), context.logDefaults(), context.topicConfig(),
            context.partitionCount(), context.metadataRevision());
    }

    @Override
    public CompletableFuture<Map<TopicIdPartition, PartitionResponse>> append(
        Map<TopicIdPartition, MemoryRecords> records, RequestLocal requestLocal) {
        // Producer state must keep the same identity when the request moves to another broker.
        // The broker SPI does not carry a stable client zone yet, so use UFK's unzoned namespace.
        return storage.write(records, DisklessClientZone.NO_ZONE);
    }

    @Override
    public CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> fetch(
        FetchParams params, Map<TopicIdPartition, FetchRequest.PartitionData> partitions) {
        return storage.fetch(params, partitions);
    }

    @Override
    public OffsetJob createOffsetJob() {
        return new UrsaOffsetJob();
    }

    @Override
    public DisklessTopicLifecycle topicLifecycle() {
        if (lifecycle == null) {
            try {
                lifecycle = new UrsaDisklessTopicLifecycle(config);
            } catch (Exception e) {
                throw new IllegalStateException("Failed to open Ursa topic lifecycle", e);
            }
        }
        return lifecycle;
    }

    @Override
    public void onTopicDeleted(String name, Uuid topicId) {
        storage.fenceDeletedTopic(name, topicId);
        storage.snapshotTrackedPartitions().stream().filter(tp -> tp.topicId().equals(topicId))
            .forEach(tp -> storage.cleanupPartition(tp, true));
    }

    @Override
    public void onTopicConfigChanged(String name, Uuid topicId, Map<String, String> configs) {
        storage.applyTopicConfig(name, topicId, configs);
    }

    @Override
    public void close() throws IOException {
        Utils.closeAll(storage, () -> {
            if (lifecycle != null) {
                try {
                    lifecycle.close();
                } catch (Exception e) {
                    throw new IOException("Failed to close Ursa topic lifecycle", e);
                }
            }
        });
    }

    private final class UrsaOffsetJob implements OffsetJob {
        private final Map<TopicIdPartition, ListOffsetsPartitionRequest> requests = new LinkedHashMap<>();
        private final Map<TopicIdPartition, CompletableFuture<FileRecordsOrError>> results = new LinkedHashMap<>();
        private final CompletableFuture<Void> cancellation = new CompletableFuture<>();

        @Override
        public boolean mustHandle(String topic) {
            return Boolean.parseBoolean(context.topicConfig().apply(topic).get("diskless.enable"));
        }

        @Override
        public CompletableFuture<FileRecordsOrError> add(TopicPartition partition, ListOffsetsPartition request) {
            Uuid topicId = context.topicId().apply(partition.topic());
            if (topicId == null || Uuid.ZERO_UUID.equals(topicId)) {
                return CompletableFuture.completedFuture(error(new UnknownTopicOrPartitionException()));
            }
            TopicIdPartition id = new TopicIdPartition(topicId, partition);
            requests.put(id, new ListOffsetsPartitionRequest(id, request.timestamp(),
                request.currentLeaderEpoch() < 0 ? Optional.empty() : Optional.of(request.currentLeaderEpoch())));
            CompletableFuture<FileRecordsOrError> result = new CompletableFuture<>();
            results.put(id, result);
            return result;
        }

        @Override
        public Future<Void> cancelHandler() {
            return cancellation;
        }

        @Override
        public void start() {
            cancellation.whenComplete((ignored, failure) -> {
                if (cancellation.isCancelled()) {
                    results.values().forEach(result -> result.cancel(false));
                }
            });
            if (requests.isEmpty() || cancellation.isCancelled()) {
                return;
            }
            try {
                storage.listOffsets(requests).whenComplete((response, failure) -> {
                    results.forEach((id, result) -> {
                        if (failure != null) {
                            result.complete(error(Errors.forException(failure).exception()));
                        } else {
                            ListOffsetsPartitionResponse offset = response.get(id);
                            if (offset == null) {
                                result.complete(error(Errors.UNKNOWN_SERVER_ERROR.exception()));
                            } else if (offset.error() != Errors.NONE) {
                                result.complete(error(offset.error().exception()));
                            } else {
                                result.complete(new FileRecordsOrError(Optional.empty(), offset.offset() < 0
                                    ? Optional.empty() : Optional.of(new FileRecords.TimestampAndOffset(
                                        offset.timestamp(), offset.offset(), Optional.of(offset.leaderEpoch())))));
                            }
                        }
                    });
                });
            } catch (RuntimeException e) {
                results.values().forEach(result -> result.complete(error(e)));
            }
        }
    }

    private static FileRecordsOrError error(Exception exception) {
        return new FileRecordsOrError(Optional.of(exception), Optional.empty());
    }
}
