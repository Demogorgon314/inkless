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
import org.apache.kafka.common.Uuid;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Immutable view of diskless topics from one committed Kafka metadata image.
 * A snapshot must not consult a newer image during a lookup. Callers obtain a fresh snapshot for
 * each provisioning operation or maintenance pass, and must not cache snapshots across requests.
 */
@FunctionalInterface
public interface DisklessMetadataSnapshot {
    /** Returns only diskless topics present in this image, resolved by immutable topic ID. */
    Optional<TopicMetadata> topic(Uuid topicId);

    /** Rejects stale names and partition numbers without looking up another topic incarnation. */
    default Optional<TopicMetadata> partition(TopicIdPartition partition) {
        return topic(partition.topicId()).filter(topic ->
            topic.name().equals(partition.topic()) &&
                partition.partition() >= 0 && partition.partition() < topic.partitionCount());
    }

    /** Topic overrides and their source revision travel together; broker defaults remain separate. */
    record TopicMetadata(Uuid topicId, String name, int partitionCount,
                         Map<String, String> configs, long sourceRevision) {
        public TopicMetadata {
            Objects.requireNonNull(topicId, "topicId");
            Objects.requireNonNull(name, "name");
            if (Uuid.ZERO_UUID.equals(topicId) || name.isBlank() || partitionCount <= 0 || sourceRevision < 0) {
                throw new IllegalArgumentException("Invalid diskless topic metadata");
            }
            configs = Map.copyOf(configs);
        }
    }
}
