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
 * expressed by separate request-driven and metadata-driven contracts.
 */
public interface DisklessTopicLifecycle extends AutoCloseable {
    /** Deletes one immutable topic incarnation; a same-name replacement has a different ID. */
    CompletableFuture<Void> deleteTopic(String topicName, Uuid topicId);

    /** Preserves request ordering: provision after KRaft creation, delete before KRaft deletion. */
    interface RequestDriven extends DisklessTopicLifecycle {
        CompletableFuture<Void> ensurePartitions(Set<PartitionRange> partitions);

        default CompletableFuture<Void> ensureTopic(String topicName, Uuid topicId, int partitions) {
            return ensurePartitions(Set.of(new PartitionRange(topicId, topicName, 0, partitions)));
        }
    }

    /**
     * Reconciles committed metadata. Implementations durably fence deleted IDs and reject updates
     * older than the stored source revision. Kafka retries across controller leadership changes.
     */
    interface MetadataDriven extends DisklessTopicLifecycle {
        /**
         * Creates or grows the topic and exactly replaces its stored configuration. Delayed older
         * revisions cannot overwrite newer state, and a deleted ID cannot be recreated.
         */
        CompletableFuture<Void> ensureTopic(String topicName, Uuid topicId, int partitions,
                                            Map<String, String> configs, long sourceRevision);

        /**
         * Includes in-progress creates and deletes with explicit Kafka ownership and source revision.
         * Storage names alone are not proof of ownership.
         */
        CompletableFuture<List<ManagedTopic>> listManagedTopics();

        /** Deletes absent topic IDs only when their stored revision is at or below imageOffset. */
        CompletableFuture<Void> sweepOrphans(Set<Uuid> liveTopicIds, long imageOffset);
    }

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
}
