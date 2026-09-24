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
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.server.util.Scheduler;
import org.apache.kafka.storage.diskless.handlers.UrsaStorageConfig;
import org.apache.kafka.storage.diskless.handlers.UrsaStorageEngineImpl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Supplier;

import io.aiven.inkless.engine.DisklessEngine;
import io.aiven.inkless.engine.DisklessEngineContext;
import io.aiven.inkless.engine.DisklessMetadataSnapshot;
import io.aiven.inkless.engine.DisklessRequestContext;

/** Bridges the broker SPI to the storage implementation copied from UFK. */
public final class UrsaDisklessEngine implements DisklessEngine {
    private static final Logger LOG = LoggerFactory.getLogger(UrsaDisklessEngine.class);
    private static final long RECONCILE_INTERVAL_MS = 30_000L;
    private final DisklessStorageEngine storage;
    private final Supplier<DisklessMetadataSnapshot> metadata;
    private final Scheduler scheduler;
    private ScheduledFuture<?> maintenance;
    private boolean closed;

    public UrsaDisklessEngine(UrsaStorageConfig config, DisklessEngineContext context) {
        this.metadata = context.metadata();
        this.scheduler = context.scheduler();
        storage = new UrsaStorageEngineImpl(context.time(), context.brokerId(), config,
            context.brokerLogDefaults(), context.metadata());
    }

    @Override
    public synchronized void start() {
        if (closed || maintenance != null) {
            throw new IllegalStateException("Engine already started or closed");
        }
        maintenance = scheduler.schedule("ursa-partition-reconciliation", this::reconcilePartitions,
            RECONCILE_INTERVAL_MS, RECONCILE_INTERVAL_MS);
    }

    private void reconcilePartitions() {
        var snapshot = metadata.get();
        for (var partition : storage.snapshotTrackedPartitions()) {
            try {
                if (snapshot.partition(partition).isEmpty()) {
                    // Only retire local handles. The controller owns durable deletion and its fencing.
                    storage.cleanupPartition(partition, false);
                }
            } catch (RuntimeException failure) {
                LOG.warn("Failed to retire partition {}; retrying on the next reconciliation", partition, failure);
            }
        }
    }

    @Override
    public CompletableFuture<Map<TopicIdPartition, PartitionResponse>> append(
        Map<TopicIdPartition, MemoryRecords> records, RequestLocal requestLocal, DisklessRequestContext requestContext) {
        // Producer state must keep the same identity when the request moves to another broker.
        // Client routing hints do not establish zone ownership. Preserve UFK's unzoned namespace
        // until zone selection and owner reconciliation are introduced together.
        return storage.write(records, DisklessClientZone.NO_ZONE);
    }

    @Override
    public CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> fetch(
        FetchParams params, Map<TopicIdPartition, FetchRequest.PartitionData> partitions) {
        return storage.fetch(params, partitions);
    }

    @Override
    public CompletableFuture<Map<TopicIdPartition, ListOffsetsResult>> listOffsets(
        Map<TopicIdPartition, ListOffsetsSpec> requests) {
        Map<TopicIdPartition, ListOffsetsPartitionRequest> translated = new LinkedHashMap<>();
        requests.forEach((partition, spec) -> translated.put(partition,
            new ListOffsetsPartitionRequest(partition, spec.timestamp(), spec.currentLeaderEpoch())));
        return storage.listOffsets(translated).thenApply(response -> {
            Map<TopicIdPartition, ListOffsetsResult> results = new LinkedHashMap<>();
            response.forEach((partition, result) -> results.put(partition, new ListOffsetsResult(
                result.error(), result.timestamp(), result.offset(),
                result.leaderEpoch() < 0 ? Optional.empty() : Optional.of(result.leaderEpoch()))));
            return results;
        });
    }

    @Override
    public void onTopicDeleted(String name, Uuid topicId) {
        storage.fenceDeletedTopic(name, topicId);
    }

    @Override
    public void onTopicConfigChanged(DisklessMetadataSnapshot.TopicMetadata topic) {
        storage.applyTopicConfig(topic.name(), topic.topicId(), topic.configs());
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        if (maintenance != null) {
            maintenance.cancel(false);
        }
        storage.close();
    }
}
