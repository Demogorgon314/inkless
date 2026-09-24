/*
 * Inkless
 * Copyright (C) 2024 - 2025 Aiven OY
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.aiven.inkless.metadata;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.Errors;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import io.aiven.inkless.control_plane.MetadataView;
import io.aiven.inkless.engine.DisklessRequestContext;
import io.aiven.inkless.engine.PartitionPlacement;
import io.aiven.inkless.engine.PartitionPlacement.PartitionAssignment;
import io.aiven.inkless.engine.PartitionPlacement.Placement;

/**
 * Applies placements to metadata responses with the rules of Kafka's DisklessMetadataRewriter, so
 * placement tests can assert on complete responses. This module cannot depend on core.
 */
final class PlacementResponseWriter {
    /** Adds the live brokers that Kafka supplies to placement. */
    interface Metadata extends MetadataView {
        List<Node> getAliveBrokerNodes(ListenerName listenerName);
    }

    private final Metadata metadataView;
    private final PartitionPlacement placement;

    PlacementResponseWriter(final Metadata metadataView, final PartitionPlacement placement) {
        this.metadataView = Objects.requireNonNull(metadataView, "metadataView cannot be null");
        this.placement = Objects.requireNonNull(placement, "placement cannot be null");
    }

    void transformClusterMetadata(final ListenerName listenerName, final String clientId,
                                  final Iterable<MetadataResponseTopic> topicMetadata) {
        Objects.requireNonNull(topicMetadata, "topicMetadata cannot be null");
        final List<PartitionAssignment> assignments = new ArrayList<>();
        final List<Consumer<Placement>> targets = new ArrayList<>();
        for (final var topic : topicMetadata) {
            if (!metadataView.isDisklessTopic(topic.name())) {
                continue;
            }
            final boolean remote = metadataView.isRemoteStorageEnabled(topic.name());
            for (final var partition : topic.partitions()) {
                assignments.add(new PartitionAssignment(topic.topicId(), partition.partitionIndex(),
                    partition.replicaNodes(), remote));
                targets.add(result -> {
                    partition.setLeaderId(result.leaderId());
                    if (result.available()) {
                        partition.setErrorCode(Errors.NONE.code());
                    }
                    partition.setReplicaNodes(new ArrayList<>(result.replicas()));
                    partition.setIsrNodes(new ArrayList<>(result.isr()));
                    partition.setOfflineReplicas(new ArrayList<>(result.offlineReplicas()));
                });
            }
        }
        apply(listenerName, clientId, assignments, targets);
    }

    void transformDescribeTopicResponse(final ListenerName listenerName, final String clientId,
                                        final DescribeTopicPartitionsResponseData responseData) {
        Objects.requireNonNull(responseData, "responseData cannot be null");
        final List<PartitionAssignment> assignments = new ArrayList<>();
        final List<Consumer<Placement>> targets = new ArrayList<>();
        for (final var topic : responseData.topics()) {
            if (!metadataView.isDisklessTopic(topic.name())) {
                continue;
            }
            final boolean remote = metadataView.isRemoteStorageEnabled(topic.name());
            for (final var partition : topic.partitions()) {
                assignments.add(new PartitionAssignment(topic.topicId(), partition.partitionIndex(),
                    partition.replicaNodes(), remote));
                targets.add(result -> {
                    partition.setLeaderId(result.leaderId());
                    if (result.available()) {
                        partition.setErrorCode(Errors.NONE.code());
                    }
                    partition.setReplicaNodes(new ArrayList<>(result.replicas()));
                    partition.setIsrNodes(new ArrayList<>(result.isr()));
                    partition.setEligibleLeaderReplicas(new ArrayList<>());
                    partition.setLastKnownElr(new ArrayList<>());
                    partition.setOfflineReplicas(new ArrayList<>(result.offlineReplicas()));
                });
            }
        }
        apply(listenerName, clientId, assignments, targets);
    }

    private void apply(final ListenerName listenerName, final String clientId,
                       final List<PartitionAssignment> assignments, final List<Consumer<Placement>> targets) {
        if (assignments.isEmpty()) {
            return;
        }
        final var client = new DisklessRequestContext(clientId, listenerName == null ? null : listenerName.value());
        final List<Placement> placements =
            placement.place(client, metadataView.getAliveBrokerNodes(listenerName), assignments);
        for (int i = 0; i < placements.size(); i++) {
            targets.get(i).accept(placements.get(i));
        }
    }
}
