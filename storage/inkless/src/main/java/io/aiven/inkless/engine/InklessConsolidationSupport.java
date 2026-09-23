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
package io.aiven.inkless.engine;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

import io.aiven.inkless.common.SharedState;
import io.aiven.inkless.consume.FetchHandler;
import io.aiven.inkless.consume.Reader;
import io.aiven.inkless.control_plane.AdvanceCrossTierLogStartOffsetRequest;
import io.aiven.inkless.control_plane.AdvanceCrossTierLogStartOffsetResponse;
import io.aiven.inkless.control_plane.ListOffsetsRequest;
import io.aiven.inkless.control_plane.PruneDisklessLogsError;
import io.aiven.inkless.control_plane.PruneDisklessLogsRequest;

final class InklessConsolidationSupport implements LogRetention, Fetcher, Closeable {
    private final SharedState state;
    private final Optional<FetchHandler> fetchHandler;

    InklessConsolidationSupport(SharedState state, Optional<InklessDisklessEngine.ConsolidationConfig> config) {
        this.state = state;
        this.fetchHandler = config.map(c -> new FetchHandler(new Reader(
            state.time(), state.objectKeyCreator(), state.keyAlignmentStrategy(), state.cache(),
            state.controlPlane(), state.fetchStorage(), state.brokerTopicStats(),
            c.metadataThreads(), c.dataThreads(),
            // Consolidation uses the background cold path and its own data pool, with no hedging.
            Optional.of(state.backgroundStorage()), state.config().fetchLaggingConsumerThresholdMs(),
            c.requestRateLimit(), 0, 0L, 0L, c.maxBatchesPerPartition(),
            new KafkaMetricsGroup("io.aiven.inkless.consolidation", "ConsolidationFetchMetrics"),
            "inkless-consolidation-", true)));
    }

    @Override
    public CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> fetch(
        FetchParams params, Map<TopicIdPartition, FetchRequest.PartitionData> partitions) {
        return fetchHandler.orElseThrow(() -> new IllegalStateException("Consolidation is disabled"))
            .handle(params, partitions);
    }

    @Override
    public OptionalLong remoteLogStartOffset(TopicIdPartition partition) {
        return state.controlPlane().getCrossTierLogStart(partition);
    }

    @Override
    public OptionalLong earliestOffset(TopicIdPartition partition) {
        Long cached = state.crossTierLogStartCache().get(partition);
        if (cached != null) {
            return OptionalLong.of(cached);
        }
        var responses = state.controlPlane().listOffsets(
            List.of(new ListOffsetsRequest(partition, ListOffsetsRequest.EARLIEST_TIMESTAMP)));
        if (responses != null && !responses.isEmpty()) {
            var response = responses.get(0);
            if (response.errors() == Errors.NONE && response.offset() >= 0) {
                state.crossTierLogStartCache().put(partition, response.offset());
                return OptionalLong.of(response.offset());
            }
        }
        return OptionalLong.empty();
    }

    @Override
    public Map<TopicIdPartition, OffsetResult> advanceEarliestOffsets(Map<TopicIdPartition, Long> offsets) {
        var partitions = List.copyOf(offsets.keySet());
        var requests = partitions.stream().map(p ->
            new AdvanceCrossTierLogStartOffsetRequest(p.topicId(), p.partition(), offsets.get(p))).toList();
        var responses = state.controlPlane().advanceCrossTierLogStartOffset(requests);
        var result = new LinkedHashMap<TopicIdPartition, OffsetResult>();
        for (int i = 0; i < responses.size(); i++) {
            var response = responses.get(i);
            var partition = partitions.get(i);
            if (response.errors() == Errors.NONE &&
                response.remoteLogStartOffset() != AdvanceCrossTierLogStartOffsetResponse.NO_OFFSET) {
                state.crossTierLogStartCache().put(partition, response.remoteLogStartOffset());
            }
            result.put(partition, new OffsetResult(response.errors(), response.remoteLogStartOffset()));
        }
        return result;
    }

    @Override
    public void reportRemoteLogStartOffset(TopicPartition partition, long offset) {
        state.crossTierLogStartReporter().enqueue(partition, offset);
    }

    @Override
    public Map<TopicIdPartition, OffsetResult> reclaimReplicatedRecords(Map<TopicIdPartition, Long> highestTieredOffsets) {
        var requests = highestTieredOffsets.entrySet().stream()
            .map(e -> new PruneDisklessLogsRequest(e.getKey(), e.getValue())).toList();
        var result = new LinkedHashMap<TopicIdPartition, OffsetResult>();
        state.controlPlane().pruneDisklessLogs(requests).forEach(r -> result.put(r.topicIdPartition(),
            new OffsetResult(r.error() == PruneDisklessLogsError.NONE ? Errors.NONE : Errors.UNKNOWN_TOPIC_OR_PARTITION,
                r.disklessLogStartOffset())));
        return result;
    }

    @Override
    public void close() throws IOException {
        if (fetchHandler.isPresent()) {
            fetchHandler.get().close();
        }
    }
}
