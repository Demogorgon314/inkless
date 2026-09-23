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
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsPartitionResult;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.util.Scheduler;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Experimental broker data-plane boundary. The engine owns record validation, offsets,
 * producer state, and persistence; Kafka owns authorization and classic/diskless routing.
 * This interface is not a complete storage-provider or stable public API.
 *
 * <p>Batch operations complete normally with one non-null result per requested partition, including
 * partition failures. They never omit failed partitions. An exceptional future means the batch
 * failed without a complete result map; it does not imply that no records were committed.
 *
 * <p>Kafka owns request deadlines. Cancellation stops waiting and may request backend cancellation;
 * an append can still commit after its caller stops waiting. Implementations must not retain
 * RequestLocal for asynchronous use, or reuse record buffers while Kafka is reading a response.
 *
 * <p>Kafka stops request handlers and the broker scheduler before close. The engine must settle
 * its own background work before releasing resources. Close is idempotent; it is not a per-request
 * cancellation API.
 * Borrowed capabilities share the engine lifetime; callers do not close them independently
 * and must not use them after engine close.
 * Callbacks may run on completion threads, so neither side may assume a request-handler thread.
 */
public interface DisklessEngine extends Appender, Fetcher, OffsetReader, Closeable {
    /** Kafka-owned services; metadata.get() captures one image for a request or maintenance pass. */
    record Context(Time time, int brokerId, BrokerTopicStats metrics,
                   Map<String, Object> logDefaults, Supplier<DisklessMetadataSnapshot> metadata) {
    }

    /** Fences deleted topic incarnations before the broker retires their partition handles. */
    default void onTopicDeleted(String name, Uuid topicId) {
    }

    /** Applies topic overrides from the same committed image as the supplied source revision. */
    default void onTopicConfigChanged(DisklessMetadataSnapshot.TopicMetadata topic) {
    }

    /** Starts engine-owned maintenance after broker construction. Called once before shutdown. */
    default void start(Scheduler scheduler, long initialDelayMs) {
    }

    /** Optional deletion support. Capability availability stays fixed for this engine's lifetime. */
    default Optional<RecordDeleter> recordDeleter() {
        return Optional.empty();
    }

    @FunctionalInterface
    interface RecordDeleter {
        /** Returns one result per partition, including partitions deleted during the request. */
        CompletableFuture<Map<TopicPartition, DeleteRecordsPartitionResult>> deleteRecords(
            Map<TopicPartition, Long> offsets);
    }

    record FetchProbe(TopicIdPartition partition, long offset, int maxBytes) { }

    /** hasData distinguishes a known batch range from a local cache miss. */
    record FetchAvailability(TopicIdPartition partition, Errors error, boolean hasData,
                             long highWatermark, long estimatedBytes) { }

    /** Optional readiness service, owned and closed by the engine. */
    default Optional<FetchProber> fetchProber() {
        return Optional.empty();
    }

    @FunctionalInterface
    interface FetchProber {
        /**
         * Returns ordered, unbudgeted readiness hints. A zero-byte result can be stale and must
         * never replace an authoritative fetch response.
         */
        List<FetchAvailability> probeFetch(List<FetchProbe> requests);
    }

    /** Optional takeover of classic logs; independent of consolidation support. */
    default Optional<LogTransitionSupport> logTransition() {
        return Optional.empty();
    }
}
