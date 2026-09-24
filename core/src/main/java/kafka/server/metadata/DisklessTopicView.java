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
package kafka.server.metadata;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.network.ListenerName;

import java.util.List;
import java.util.Set;

/**
 * Diskless topic state that Kafka reads from its own metadata to route requests, independently of
 * the storage engine. Every lookup reads the broker's current metadata image.
 */
public interface DisklessTopicView {
    List<Node> getAliveBrokerNodes(ListenerName listenerName);

    /** Returns the topic ID, or null or {@link Uuid#ZERO_UUID} when the topic is unknown. */
    Uuid getTopicId(String topicName);

    boolean isDisklessTopic(String topicName);

    boolean isRemoteStorageEnabled(String topicName);

    /** True if the topic is diskless and keeps a Kafka log tier; false otherwise. */
    boolean isConsolidatingDisklessTopic(String topicName);

    Set<TopicIdPartition> getDisklessTopicPartitions();

    Set<TopicIdPartition> getConsolidatingDisklessTopicPartitions();

    /** Returns the classic-to-diskless seal, or one of the negative sentinels in PartitionRegistration. */
    long getClassicToDisklessStartOffset(TopicPartition topicPartition);

    /** Returns the leader epoch captured at the switch, or PartitionRegistration.NO_DISKLESS_LEADER_EPOCH. */
    int getDisklessLeaderEpoch(TopicPartition topicPartition);

    boolean isReplicaInIsr(TopicPartition topicPartition, int replicaId);
}
