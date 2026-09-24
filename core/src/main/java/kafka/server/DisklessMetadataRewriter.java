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
package kafka.server;

import kafka.server.metadata.DisklessTopicView;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponsePartition;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponsePartition;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.Errors;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import io.aiven.inkless.engine.DisklessRequestContext;
import io.aiven.inkless.engine.PartitionPlacement;
import io.aiven.inkless.engine.PartitionPlacement.PartitionAssignment;
import io.aiven.inkless.engine.PartitionPlacement.Placement;

/**
 * Rewrites diskless partitions in Metadata and DescribeTopicPartitions responses with the brokers
 * that the engine's placement chooses. Kafka keeps the KRaft leader epoch, so clients still fence
 * on it, and keeps the KRaft partition error when placement finds no available broker.
 */
public final class DisklessMetadataRewriter {
    private final DisklessTopicView metadataView;
    private final PartitionPlacement placement;

    public DisklessMetadataRewriter(DisklessTopicView metadataView, PartitionPlacement placement) {
        this.metadataView = Objects.requireNonNull(metadataView, "metadataView cannot be null");
        this.placement = Objects.requireNonNull(placement, "placement cannot be null");
    }

    /**
     * Rewrites the diskless partitions of a Metadata response.
     *
     * @param clientId client ID, {@code null} if not provided.
     */
    public void transformClusterMetadata(ListenerName listenerName, String clientId,
                                         Iterable<MetadataResponseTopic> topicMetadata) {
        Objects.requireNonNull(topicMetadata, "topicMetadata cannot be null");
        var batch = new Batch();
        for (var topic : topicMetadata) {
            if (!metadataView.isDisklessTopic(topic.name())) {
                continue;
            }
            boolean remoteStorageEnabled = metadataView.isRemoteStorageEnabled(topic.name());
            for (var partition : topic.partitions()) {
                batch.add(new PartitionAssignment(topic.topicId(), partition.partitionIndex(),
                    partition.replicaNodes(), remoteStorageEnabled), result -> apply(partition, result));
            }
        }
        batch.place(listenerName, clientId);
    }

    /**
     * Rewrites the diskless partitions of a DescribeTopicPartitions response.
     *
     * @param clientId client ID, {@code null} if not provided.
     */
    public void transformDescribeTopicResponse(ListenerName listenerName, String clientId,
                                               DescribeTopicPartitionsResponseData responseData) {
        Objects.requireNonNull(responseData, "responseData cannot be null");
        var batch = new Batch();
        for (var topic : responseData.topics()) {
            if (!metadataView.isDisklessTopic(topic.name())) {
                continue;
            }
            boolean remoteStorageEnabled = metadataView.isRemoteStorageEnabled(topic.name());
            for (var partition : topic.partitions()) {
                batch.add(new PartitionAssignment(topic.topicId(), partition.partitionIndex(),
                    partition.replicaNodes(), remoteStorageEnabled), result -> apply(partition, result));
            }
        }
        batch.place(listenerName, clientId);
    }

    private static void apply(MetadataResponsePartition partition, Placement placement) {
        partition.setLeaderId(placement.leaderId());
        if (placement.available()) {
            partition.setErrorCode(Errors.NONE.code());
        }
        partition.setReplicaNodes(new ArrayList<>(placement.replicas()));
        partition.setIsrNodes(new ArrayList<>(placement.isr()));
        partition.setOfflineReplicas(new ArrayList<>(placement.offlineReplicas()));
    }

    private static void apply(DescribeTopicPartitionsResponsePartition partition, Placement placement) {
        partition.setLeaderId(placement.leaderId());
        if (placement.available()) {
            partition.setErrorCode(Errors.NONE.code());
        }
        partition.setReplicaNodes(new ArrayList<>(placement.replicas()));
        partition.setIsrNodes(new ArrayList<>(placement.isr()));
        partition.setEligibleLeaderReplicas(new ArrayList<>());
        partition.setLastKnownElr(new ArrayList<>());
        partition.setOfflineReplicas(new ArrayList<>(placement.offlineReplicas()));
    }

    /** Collects every diskless partition of one response, so placement runs once per request. */
    private final class Batch {
        private final List<PartitionAssignment> assignments = new ArrayList<>();
        private final List<Consumer<Placement>> targets = new ArrayList<>();

        void add(PartitionAssignment assignment, Consumer<Placement> target) {
            assignments.add(assignment);
            targets.add(target);
        }

        void place(ListenerName listenerName, String clientId) {
            if (assignments.isEmpty()) {
                return;
            }
            List<Node> aliveBrokers = metadataView.getAliveBrokerNodes(listenerName);
            var client = new DisklessRequestContext(clientId, listenerName == null ? null : listenerName.value());
            List<Placement> placements = placement.place(client, aliveBrokers, assignments);
            if (placements.size() != assignments.size()) {
                throw new IllegalStateException("Placement returned " + placements.size()
                    + " results for " + assignments.size() + " partitions");
            }
            for (int i = 0; i < placements.size(); i++) {
                targets.get(i).accept(placements.get(i));
            }
        }
    }
}
