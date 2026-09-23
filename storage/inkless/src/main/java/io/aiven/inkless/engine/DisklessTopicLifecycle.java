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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Storage-neutral lifecycle operations for Kafka topics whose data is managed by a diskless
 * storage implementation.
 *
 * <p>The implementation owns its catalog schema and metadata-store layout. Kafka supplies only
 * topic identity, partition count, and topic configuration. Every operation is idempotent: the
 * controller retries it either through the request path or through metadata reconciliation, as
 * selected by {@link #executionMode()}. A provider must not opt into reconciliation without its durable guarantees.
 */
public interface DisklessTopicLifecycle extends AutoCloseable {
    enum ExecutionMode {
        /** Provision after KRaft creation; delete storage before removing KRaft metadata. */
        REQUEST_DRIVEN,
        /** Reconcile committed metadata, with durable revision checks and deletion fences. */
        METADATA_DRIVEN
    }

    ExecutionMode executionMode();

    /** A half-open range of newly diskless partitions; excluded migrating partitions stay untouched. */
    record PartitionRange(Uuid topicId, String topicName, int firstPartition, int partitionLimit) {
        public PartitionRange {
            Objects.requireNonNull(topicId, "topicId must not be null");
            Objects.requireNonNull(topicName, "topicName must not be null");
            if (firstPartition < 0 || partitionLimit < firstPartition) {
                throw new IllegalArgumentException("Invalid partition range");
            }
        }
    }

    /**
     * Provisions explicit partition ranges after a request-driven partition increase. The broker
     * initializes migrated partitions separately from their committed seal and producer state.
     * Metadata-driven providers receive their desired layout through ensureTopic instead.
     */
    default CompletableFuture<Void> ensurePartitions(Set<PartitionRange> partitions) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Partition-range provisioning is unavailable"));
    }

    /**
     * A Kafka topic incarnation durably owned by this diskless storage implementation.
     *
     * <p>The topic ID, rather than the topic name alone, identifies the immutable Kafka topic
     * incarnation. Implementations must only return entries carrying explicit Kafka ownership
     * metadata; an arbitrary storage object whose name resembles a Kafka topic is not managed.
     */
    record ManagedTopic(String topicName, Uuid topicId, long sourceRevision) {
        public ManagedTopic {
            Objects.requireNonNull(topicName, "topicName must not be null");
            Objects.requireNonNull(topicId, "topicId must not be null");
            if (topicName.isBlank()) {
                throw new IllegalArgumentException("topicName must not be blank");
            }
            if (Uuid.ZERO_UUID.equals(topicId)) {
                throw new IllegalArgumentException("topicId must not be zero");
            }
            if (sourceRevision < 0) {
                throw new IllegalArgumentException("sourceRevision must not be negative");
            }
        }
    }

    /**
     * Ensures one logical diskless topic exists at the supplied KRaft metadata revision.
     *
     * <p>The implementation creates the topic when it is absent, grows its partition layout when
     * needed, and applies the configuration it owns. Metadata-driven providers exactly replace
     * their stored configuration and use sourceRevision to reject delayed older updates.
     * Request-driven providers may keep configuration solely in Kafka metadata.
     */
    CompletableFuture<Void> ensureTopic(String topicName, Uuid topicId, int partitions,
                                        Map<String, String> configs, long sourceRevision);

    /**
     * Deletes the logical topic at the point selected by executionMode().
     *
     * <p>Kafka topic IDs identify immutable topic incarnations. Implementations must durably fence
     * this ID in METADATA_DRIVEN mode so an in-flight create cannot recreate it after completion.
     * REQUEST_DRIVEN mode deletes storage before KRaft metadata and retains request retry semantics;
     * it does not claim a durable fence. A same-name replacement always has a different topic ID.
     */
    CompletableFuture<Void> deleteTopic(String topicName, Uuid topicId);

    /**
     * Lists non-terminal Kafka topic incarnations for metadata-driven reconciliation.
     *
     * <p>The active Kafka controller uses this semantic inventory to reconcile storage objects
     * left behind by a controller restart. The inventory must include objects still being created
     * or deleted so an abandoned lifecycle claim cannot remain hidden. Implementations must filter
     * using durable ownership metadata and must not infer ownership from a storage-specific name
     * alone. Each entry must also carry the KRaft source revision from the reconciliation that made
     * it visible, so a newly elected but lagging controller cannot delete newer state.
     */
    default CompletableFuture<List<ManagedTopic>> listManagedTopics() {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Managed inventory is unavailable"));
    }

    /**
     * Deletes managed topics absent from the controller image in metadata-driven mode.
     *
     * <p>{@code liveTopicIds} is the set of diskless topic IDs present in the image at
     * {@code imageOffset}. An implementation must only delete an entry whose source revision is at
     * or below {@code imageOffset}, so state created from a newer image than the caller has seen
     * survives.
     */
    default CompletableFuture<Void> sweepOrphans(Set<Uuid> liveTopicIds, long imageOffset) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("Orphan reconciliation is unavailable"));
    }
}
