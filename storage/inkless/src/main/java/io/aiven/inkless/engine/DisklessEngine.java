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

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsPartitionResult;
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.server.util.Scheduler;
import org.apache.kafka.storage.internals.log.OffsetResultHolder.FileRecordsOrError;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * Experimental broker data-plane boundary. The engine owns record validation, offsets,
 * producer state, and persistence; Kafka owns authorization and classic/diskless routing.
 * This interface is not a complete storage-provider or stable public API.
 */
public interface DisklessEngine extends Configurable, Closeable {
    record Context(Time time, int brokerId, BrokerTopicStats metrics,
                   Map<String, Object> logDefaults, Function<String, Uuid> topicId,
                   Function<String, Map<String, String>> topicConfig,
                   Function<String, OptionalInt> partitionCount, LongSupplier metadataRevision) {
    }

    /** Initializes broker services after configure and before the first data request. */
    default void initialize(Context context) {
    }

    /** Creates the controller service; the engine owns and closes it. No broker context is required. */
    default DisklessTopicLifecycle topicLifecycle() {
        throw new UnsupportedOperationException("This engine has no controller lifecycle");
    }

    /** Fences deleted topic incarnations before the broker retires their partition handles. */
    default void onTopicDeleted(String name, Uuid topicId) {
    }

    default void onTopicConfigChanged(String name, Uuid topicId, Map<String, String> config) {
    }

    /**
     * Completes after the engine commits the records. Uses RequestLocal only on the calling
     * thread; asynchronous work must own any buffers it retains beyond the call.
     */
    CompletableFuture<Map<TopicIdPartition, PartitionResponse>> append(
        Map<TopicIdPartition, MemoryRecords> records, RequestLocal requestLocal);

    /**
     * Preserves request iteration order when spending the shared fetch byte budget.
     * Returns a result for every requested partition, including partition-level failures.
     */
    CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> fetch(
        FetchParams params, Map<TopicIdPartition, FetchRequest.PartitionData> partitions);

    OffsetJob createOffsetJob();

    /** Starts engine-owned maintenance after broker construction. Called once before shutdown. */
    default void start(Scheduler scheduler, long initialDelayMs) {
    }

    default boolean supportsDeleteRecords() {
        return false;
    }

    /** Returns one result per partition; implementations must complete even if a topic disappears. */
    default CompletableFuture<Map<TopicPartition, DeleteRecordsPartitionResult>> deleteRecords(
        Map<TopicPartition, Long> offsets) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("DeleteRecords is unavailable"));
    }

    record FetchProbe(TopicIdPartition partition, long offset, int maxBytes) { }

    /** hasData distinguishes a known batch range from a local cache miss. */
    record FetchAvailability(TopicIdPartition partition, Errors error, boolean hasData,
                             long highWatermark, long estimatedBytes) { }

    /**
     * Best-effort, unbudgeted readiness hint. Empty means the broker must attempt asynchronous fetch.
     * A zero-byte result can be stale and must never replace the authoritative fetch response.
     */
    default Optional<List<FetchAvailability>> probeFetch(List<FetchProbe> requests) {
        return Optional.empty();
    }

    /** Optional cooperation with Kafka's local and remote tiers; absent for standalone engines. */
    default Optional<TieredStorage> tieredStorage() {
        return Optional.empty();
    }

    /** A nonnegative offset is usable only with NONE; a negative offset means no value is available. */
    record OffsetResult(Errors error, long offset) { }

    record ProducerState(long producerId, short producerEpoch, int baseSequence, int lastSequence,
                         long assignedOffset, long batchMaxTimestamp) { }

    record LogInitialization(Uuid topicId, String topicName, int partition,
                             long logStartOffset, long disklessStartOffset, List<ProducerState> producerStates) { }

    /** Asynchronous record fetch with the same ordering and ownership contract as engine fetch. */
    @FunctionalInterface
    interface Fetcher {
        CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> handle(
            FetchParams params, Map<TopicIdPartition, FetchRequest.PartitionData> partitions);
    }

    /**
     * Kafka owns local-log coordination and retries; the engine owns external metadata and caches.
     * Metadata methods retain the native synchronous contract; callers must not treat them as nonblocking.
     * Returned services are owned and closed by the engine, never by their callers.
     */
    interface TieredStorage extends Fetcher {
        /**
         * Returns only the remote start reported by the local-log leader. Never substitutes the
         * diskless prune frontier: Kafka uses this value to decide which remote data it can reclaim.
         */
        OptionalLong remoteLogStartOffset(TopicIdPartition partition);

        /** Returns the cross-tier logical earliest offset, independently of this broker's local log. */
        OptionalLong earliestOffset(TopicIdPartition partition);

        /** Advances logical earliest offsets monotonically; does not physically delete remote-tier data. */
        Map<TopicIdPartition, OffsetResult> advanceEarliestOffsets(Map<TopicIdPartition, Long> offsets);

        /** Queues the leader's remote-start report for engine-owned persistence. */
        void reportRemoteLogStartOffset(TopicPartition partition, long offset);

        /** Prunes through Kafka's inclusive safe tiered offsets; results contain the new diskless starts. */
        Map<TopicIdPartition, OffsetResult> prune(Map<TopicIdPartition, Long> highestTieredOffsets);

        long cleanupIntervalMs();

        /**
         * Applies the seal and producer state after KRaft commits the transition, on the leader only.
         * Returns outcomes in request order; INVALID_REQUEST means already initialized.
         */
        List<Errors> initializeLogs(List<LogInitialization> requests);

        /** Reconciles existing external log metadata with the seal committed in KRaft. */
        Errors repairLog(TopicIdPartition partition, long startOffset);
    }

    /**
     * Batches lookups before start and exposes cancellation to Kafka's ListOffsets purgatory.
     * Each request, including a delayed hybrid-read fallback, needs a fresh job.
     * This shape preserves the existing broker integration for the proof of concept.
     */
    interface OffsetJob {
        boolean mustHandle(String topic);

        CompletableFuture<FileRecordsOrError> add(TopicPartition partition, ListOffsetsPartition request);

        Future<Void> cancelHandler();

        void start();
    }
}
