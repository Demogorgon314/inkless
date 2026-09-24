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

import org.apache.kafka.common.Uuid;

import java.io.Closeable;
import java.util.Optional;

/**
 * Experimental broker data-plane boundary. The engine owns record validation, offsets,
 * producer state, and persistence; Kafka owns authorization and classic/diskless routing.
 * This interface is not a complete storage-provider or stable public API.
 *
 * <p>Every engine implements append, fetch, and offset lookup. Optional behavior is exposed
 * through the extension accessors. Kafka reads each accessor once after construction, so an
 * accessor must return the same value for the engine's lifetime. An absent extension means Kafka
 * never starts the workflows that depend on it.
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
 * cancellation API. Callbacks may run on completion threads, so neither side may assume a
 * request-handler thread.
 */
public interface DisklessEngine extends Appender, Fetcher, OffsetReader, Closeable {
    /**
     * Returns the broker routing that Kafka advertises for diskless partitions. Append must accept
     * the writes this placement routes to the local broker, and may reject writes it routes elsewhere.
     */
    PartitionPlacement placement();

    /**
     * Starts engine-owned maintenance on the context scheduler. Kafka calls it once, after the
     * broker finishes constructing its request path and before it serves diskless requests.
     */
    default void start() {
    }

    /**
     * Fences a deleted topic incarnation before the broker retires its partitions. Runs on the
     * metadata publisher thread: it must return promptly and defer remote I/O to engine threads.
     */
    default void onTopicDeleted(String name, Uuid topicId) {
    }

    /**
     * Applies topic overrides from the committed image that produced the supplied revision.
     * Runs on the metadata publisher thread under the same constraints as onTopicDeleted.
     */
    default void onTopicConfigChanged(DisklessMetadataSnapshot.TopicMetadata topic) {
    }

    /**
     * Applies a dynamic change to the broker log defaults; the context's brokerLogDefaults supplier
     * already returns the new values. Runs under the same constraints as onTopicDeleted.
     */
    default void onBrokerLogDefaultsChanged() {
    }

    /** Returns non-authoritative readiness hints that let Kafka park fetches in its purgatory. */
    default Optional<FetchProbing> fetchProbing() {
        return Optional.empty();
    }

    /** Returns logical record deletion for DeleteRecords requests. */
    default Optional<RecordDeletion> recordDeletion() {
        return Optional.empty();
    }

    /** Returns copying of diskless records into Kafka's log tiers and the cross-tier offsets. */
    default Optional<LogTiering> logTiering() {
        return Optional.empty();
    }

    /** Returns takeover of classic logs after Kafka commits a classic-to-diskless transition. */
    default Optional<LogTransition> logTransition() {
        return Optional.empty();
    }
}
