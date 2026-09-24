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
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponsePartition;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponseTopic;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponseTopicCollection;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponsePartition;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.Errors;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import io.aiven.inkless.engine.DisklessRequestContext;
import io.aiven.inkless.engine.PartitionPlacement;
import io.aiven.inkless.engine.PartitionPlacement.PartitionAssignment;
import io.aiven.inkless.engine.PartitionPlacement.Placement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DisklessMetadataRewriterTest {
    private static final ListenerName LISTENER = ListenerName.normalised("external");
    private static final List<Node> BROKERS = List.of(new Node(1, "b1", 9092, "az-a"), new Node(2, "b2", 9092, "az-b"));
    private static final Uuid DISKLESS_ID = Uuid.randomUuid();
    private static final Uuid TIERED_ID = Uuid.randomUuid();

    private final DisklessTopicView metadataView = mock(DisklessTopicView.class);

    /** Records every call and returns the placements supplied by the test. */
    private static final class RecordingPlacement implements PartitionPlacement {
        final List<DisklessRequestContext> clients = new ArrayList<>();
        final List<List<Node>> brokers = new ArrayList<>();
        final List<List<PartitionAssignment>> assignments = new ArrayList<>();
        private final List<Placement> results;

        RecordingPlacement(List<Placement> results) {
            this.results = results;
        }

        @Override
        public List<Placement> place(DisklessRequestContext client, List<Node> aliveBrokers,
                                     List<PartitionAssignment> partitions) {
            clients.add(client);
            brokers.add(aliveBrokers);
            assignments.add(partitions);
            return results;
        }
    }

    private static MetadataResponsePartition metadataPartition(int index, short error) {
        return new MetadataResponsePartition().setPartitionIndex(index).setErrorCode(error)
            .setLeaderId(9).setLeaderEpoch(7).setReplicaNodes(List.of(1, 2)).setIsrNodes(List.of(1, 2))
            .setOfflineReplicas(List.of());
    }

    @Test
    public void rewritesOnlyDisklessPartitionsWithOnePlacementCall() {
        when(metadataView.isDisklessTopic("diskless")).thenReturn(true);
        when(metadataView.isDisklessTopic("tiered")).thenReturn(true);
        when(metadataView.isDisklessTopic("classic")).thenReturn(false);
        when(metadataView.isRemoteStorageEnabled("tiered")).thenReturn(true);
        when(metadataView.getAliveBrokerNodes(LISTENER)).thenReturn(BROKERS);
        var placement = new RecordingPlacement(List.of(
            new Placement(2, List.of(2), List.of(2), List.of()),
            new Placement(1, List.of(1, 2), List.of(1), List.of(2))));
        var classic = metadataPartition(0, Errors.NONE.code());
        var topics = List.of(
            new MetadataResponseTopic().setName("diskless").setTopicId(DISKLESS_ID)
                .setPartitions(List.of(metadataPartition(0, Errors.LEADER_NOT_AVAILABLE.code()))),
            new MetadataResponseTopic().setName("classic").setTopicId(Uuid.randomUuid())
                .setPartitions(List.of(classic)),
            new MetadataResponseTopic().setName("tiered").setTopicId(TIERED_ID)
                .setPartitions(List.of(metadataPartition(3, Errors.NONE.code()))));

        new DisklessMetadataRewriter(metadataView, placement).transformClusterMetadata(LISTENER, "client-1", topics);

        assertEquals(List.of(new DisklessRequestContext("client-1", LISTENER.value())), placement.clients);
        assertEquals(List.of(BROKERS), placement.brokers);
        assertEquals(List.of(List.of(
            new PartitionAssignment(DISKLESS_ID, 0, List.of(1, 2), false),
            new PartitionAssignment(TIERED_ID, 3, List.of(1, 2), true))), placement.assignments);

        var diskless = topics.get(0).partitions().get(0);
        assertEquals(2, diskless.leaderId());
        assertEquals(Errors.NONE.code(), diskless.errorCode());
        assertEquals(List.of(2), diskless.replicaNodes());
        assertEquals(List.of(2), diskless.isrNodes());
        assertEquals(7, diskless.leaderEpoch(), "Kafka keeps the KRaft leader epoch");
        var tiered = topics.get(2).partitions().get(0);
        assertEquals(List.of(1), tiered.isrNodes());
        assertEquals(List.of(2), tiered.offlineReplicas());
        assertEquals(metadataPartition(0, Errors.NONE.code()), classic);
    }

    @Test
    public void keepsKraftErrorWhenPlacementFindsNoBroker() {
        when(metadataView.isDisklessTopic("diskless")).thenReturn(true);
        when(metadataView.getAliveBrokerNodes(LISTENER)).thenReturn(List.of());
        var placement = new RecordingPlacement(List.of(new Placement(-1, List.of(1, 2), List.of(), List.of(1, 2))));
        var partition = metadataPartition(0, Errors.LEADER_NOT_AVAILABLE.code());

        new DisklessMetadataRewriter(metadataView, placement).transformClusterMetadata(LISTENER, null,
            List.of(new MetadataResponseTopic().setName("diskless").setTopicId(DISKLESS_ID).setPartitions(List.of(partition))));

        assertEquals(-1, partition.leaderId());
        assertEquals(Errors.LEADER_NOT_AVAILABLE.code(), partition.errorCode());
        assertEquals(List.of(1, 2), partition.offlineReplicas());
        assertEquals(new DisklessRequestContext(null, LISTENER.value()), placement.clients.get(0));
    }

    @Test
    public void describeResponseClearsEligibleLeaderReplicas() {
        when(metadataView.isDisklessTopic("diskless")).thenReturn(true);
        when(metadataView.getAliveBrokerNodes(LISTENER)).thenReturn(BROKERS);
        var placement = new RecordingPlacement(List.of(new Placement(1, List.of(1), List.of(1), List.of())));
        var partition = new DescribeTopicPartitionsResponsePartition().setPartitionIndex(0)
            .setLeaderId(9).setReplicaNodes(List.of(1, 2)).setIsrNodes(List.of(1, 2))
            .setEligibleLeaderReplicas(List.of(2)).setLastKnownElr(List.of(2));
        var response = new DescribeTopicPartitionsResponseData().setTopics(
            new DescribeTopicPartitionsResponseTopicCollection(List.of(new DescribeTopicPartitionsResponseTopic()
                .setName("diskless").setTopicId(DISKLESS_ID).setPartitions(List.of(partition))).iterator()));

        new DisklessMetadataRewriter(metadataView, placement).transformDescribeTopicResponse(LISTENER, "c", response);

        assertEquals(1, partition.leaderId());
        assertEquals(List.of(1), partition.replicaNodes());
        assertTrue(partition.eligibleLeaderReplicas().isEmpty());
        assertTrue(partition.lastKnownElr().isEmpty());
    }

    @Test
    public void skipsPlacementWithoutDisklessPartitions() {
        var placement = new RecordingPlacement(List.of());
        new DisklessMetadataRewriter(metadataView, placement).transformClusterMetadata(LISTENER, "c",
            List.of(new MetadataResponseTopic().setName("classic").setPartitions(List.of(metadataPartition(0, (short) 0)))));
        assertTrue(placement.clients.isEmpty());
        verify(metadataView, never()).getAliveBrokerNodes(any());
    }

    @Test
    public void rejectsPlacementThatOmitsPartitions() {
        when(metadataView.isDisklessTopic("diskless")).thenReturn(true);
        when(metadataView.getAliveBrokerNodes(LISTENER)).thenReturn(BROKERS);
        var rewriter = new DisklessMetadataRewriter(metadataView, new RecordingPlacement(List.of()));
        assertThrows(IllegalStateException.class, () -> rewriter.transformClusterMetadata(LISTENER, "c",
            List.of(new MetadataResponseTopic().setName("diskless").setTopicId(DISKLESS_ID)
                .setPartitions(List.of(metadataPartition(0, (short) 0))))));
    }
}
