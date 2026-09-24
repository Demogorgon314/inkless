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
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Utils;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import io.aiven.inkless.engine.DisklessRequestContext;
import io.aiven.inkless.engine.PartitionPlacement;

/**
 * Routes clients of diskless partitions to brokers in their AZ. Inkless accepts writes on every
 * broker, so it spreads partitions across brokers by hash.
 *
 * <p>For managed replicas (RF > 1), routing priority depends on remote storage config:
 * <ul>
 *   <li>Diskless and Tiered topic: same-AZ replica > cross-AZ replica > unavailable</li>
 *   <li>Only Diskless (remote storage disabled): same-AZ replica > same-AZ any broker > cross-AZ replica > cross-AZ any broker</li>
 * </ul>
 *
 * <p>For unmanaged replicas (RF = 1), uses legacy behavior: hash-based selection from all alive brokers.
 */
public class InklessPartitionPlacement implements PartitionPlacement, Closeable {
    private final Map<String, String> clientAzListenerMap;
    private final ClientAzAwarenessMetrics metrics;

    public InklessPartitionPlacement(final Map<String, String> clientAzListenerMap) {
        this.clientAzListenerMap =
            Objects.requireNonNull(clientAzListenerMap, "clientAzListenerMap cannot be null");
        metrics = new ClientAzAwarenessMetrics();
    }

    @Override
    public List<Placement> place(
        final DisklessRequestContext client,
        final List<Node> aliveBrokers,
        final List<PartitionAssignment> partitions
    ) {
        final String clientAZ = resolveClientAZ(client.listenerName(), client.clientId(), aliveBrokers);
        final Set<Integer> aliveBrokerIds = aliveBrokers.stream().map(Node::id).collect(Collectors.toSet());
        final Map<Integer, String> brokerRacks = aliveBrokers.stream()
            .collect(Collectors.toMap(Node::id, n -> n.rack() != null ? n.rack() : ""));

        final List<Placement> placements = new ArrayList<>(partitions.size());
        for (final var partition : partitions) {
            final List<Integer> assignedReplicas = partition.replicas();
            if (assignedReplicas.size() > 1) {
                final LeaderSelectionResult result = selectLeaderForManagedReplicas(
                    clientAZ, partition.topicId(), partition.partition(), assignedReplicas,
                    partition.remoteStorageEnabled(), aliveBrokerIds, brokerRacks
                );
                // Keep original replicaNodes - show real RF to clients
                placements.add(new Placement(result.leaderId(), assignedReplicas,
                    result.aliveReplicas(), result.offlineReplicas()));
            } else {
                // RF=1 (unmanaged): use legacy behavior
                final int selectedLeader = selectLeaderLegacy(clientAZ, partition.topicId(), partition.partition(), aliveBrokers);
                final List<Integer> singleReplica = List.of(selectedLeader);
                placements.add(new Placement(selectedLeader, singleReplica, singleReplica, List.of()));
            }
        }
        return placements;
    }

    /**
     * Select leader for managed replicas.
     *
     * <p>When remote storage is enabled: same-AZ replica > cross-AZ replica > unavailable
     * <p>When remote storage is disabled: same-AZ replica > same-AZ any broker > cross-AZ replica > cross-AZ any broker
     */
    private LeaderSelectionResult selectLeaderForManagedReplicas(
        final String clientAZ,
        final Uuid topicId,
        final int partitionIndex,
        final List<Integer> assignedReplicas,
        final boolean isRemoteStorageEnabled,
        final Set<Integer> aliveBrokerIds,
        final Map<Integer, String> brokerRacks
    ) {
        // Compute broker sets for routing decisions
        final List<Integer> aliveReplicas = assignedReplicas.stream()
            .filter(aliveBrokerIds::contains)
            .toList();
        final List<Integer> offlineReplicas = assignedReplicas.stream()
            .filter(id -> !aliveBrokerIds.contains(id))
            .toList();
        final List<Integer> aliveReplicasInClientAZ = filterByAZ(brokerRacks, aliveReplicas, clientAZ);
        final List<Integer> allAliveBrokersInClientAZ = filterByAZ(brokerRacks, new ArrayList<>(aliveBrokerIds), clientAZ);

        final int selectedLeader;

        // Track if any replicas are offline but we can still route
        final boolean hasOfflineReplicas = aliveReplicas.size() < assignedReplicas.size();

        if (isRemoteStorageEnabled) {
            // Remote storage enabled: must stay on replicas (RLM requires UnifiedLog)
            // Priority: same-AZ replica > cross-AZ replica > unavailable

            if (!aliveReplicasInClientAZ.isEmpty()) {
                // Best: replica in same AZ
                selectedLeader = selectByHash(topicId, partitionIndex, aliveReplicasInClientAZ);
                metrics.recordClientAz(clientAZ, true);
                if (hasOfflineReplicas) {
                    metrics.recordOfflineReplicasRoutedAround();
                }
            } else if (!aliveReplicas.isEmpty()) {
                // Fallback: replica in other AZ (cross-AZ)
                selectedLeader = selectByHash(topicId, partitionIndex, aliveReplicas);
                metrics.recordClientAz(clientAZ, false);
                recordCrossAzIfConfirmed(clientAZ, selectedLeader, brokerRacks);
                if (hasOfflineReplicas) {
                    metrics.recordOfflineReplicasRoutedAround();
                }
            } else {
                // All replicas offline — partition unavailable (Kafka semantics)
                // Cannot fall back to non-replica brokers even in same AZ
                // because tiered reads REQUIRE replica brokers with UnifiedLog/RLM state.
                // Use -1 to denote no leader; do not advertise an offline broker.
                selectedLeader = -1;
            }
        } else {
            // Remote storage disabled: can fall back to any broker for availability
            // Priority: same-AZ replica > same-AZ any broker > cross-AZ replica > cross-AZ any broker

            if (!aliveReplicasInClientAZ.isEmpty()) {
                // Best: assigned replica in same AZ
                selectedLeader = selectByHash(topicId, partitionIndex, aliveReplicasInClientAZ);
                metrics.recordClientAz(clientAZ, true);
                if (hasOfflineReplicas) {
                    metrics.recordOfflineReplicasRoutedAround();
                }
            } else if (!allAliveBrokersInClientAZ.isEmpty()) {
                // No alive replicas in client AZ, but other brokers in AZ are alive; use same-AZ non-replica
                selectedLeader = selectByHash(topicId, partitionIndex, allAliveBrokersInClientAZ);
                metrics.recordClientAz(clientAZ, true);
                metrics.recordFallback();
                if (hasOfflineReplicas) {
                    metrics.recordOfflineReplicasRoutedAround();
                }
            } else if (!aliveReplicas.isEmpty()) {
                // No brokers in client AZ available; use replica in other AZ (cross-AZ)
                selectedLeader = selectByHash(topicId, partitionIndex, aliveReplicas);
                metrics.recordClientAz(clientAZ, false);
                recordCrossAzIfConfirmed(clientAZ, selectedLeader, brokerRacks);
                if (hasOfflineReplicas) {
                    metrics.recordOfflineReplicasRoutedAround();
                }
            } else {
                // Absolute last resort: any alive broker (cross-AZ, non-replica)
                final List<Integer> allAliveBrokers = new ArrayList<>(aliveBrokerIds);
                if (!allAliveBrokers.isEmpty()) {
                    selectedLeader = selectByHash(topicId, partitionIndex, allAliveBrokers);
                    metrics.recordClientAz(clientAZ, false);
                    metrics.recordFallback();
                    recordCrossAzIfConfirmed(clientAZ, selectedLeader, brokerRacks);
                    metrics.recordOfflineReplicasRoutedAround();
                } else {
                    // No brokers at all (shouldn't happen in normal operation)
                    selectedLeader = -1;
                }
            }
        }

        return new LeaderSelectionResult(selectedLeader, aliveReplicas, offlineReplicas);
    }

    /**
     * Legacy leader selection for unmanaged (RF=1) diskless partitions.
     * Selects from all alive brokers, preferring those in the client's AZ.
     */
    private int selectLeaderLegacy(
        final String clientAZ,
        final Uuid topicId,
        final int partitionIndex,
        final List<Node> aliveNodes
    ) {
        final List<Node> sortedAlive = aliveNodes.stream()
            .sorted(Comparator.comparing(Node::id))
            .toList();

        // When clientAZ is null (unknown), skip AZ filtering — use all brokers equally
        final List<Node> brokersInClientAZ = clientAZ == null
            ? Collections.emptyList()
            : sortedAlive.stream()
                .filter(n -> clientAZ.equals(n.rack()))
                .toList();

        // Fall back on all brokers if no broker in the client AZ
        final List<Node> brokersToPickFrom = brokersInClientAZ.isEmpty()
            ? sortedAlive
            : brokersInClientAZ;

        metrics.recordClientAz(clientAZ, !brokersInClientAZ.isEmpty());

        // This cannot happen in a normal broker run. This will serve as a guard in tests.
        if (brokersToPickFrom.isEmpty()) {
            throw new RuntimeException("No broker found, unexpected");
        }

        final byte[] input = String.format("%s-%s", topicId, partitionIndex).getBytes(StandardCharsets.UTF_8);
        final int hash = Utils.murmur2(input);

        return brokersToPickFrom.get(Utils.toPositive(hash) % brokersToPickFrom.size()).id();
    }

    private static String normalizeAZ(final String az) {
        return (az == null || az.isBlank()) ? null : az;
    }

    /**
     * Resolve the client AZ using based on client ID or listener name.
     */
    private String resolveClientAZ(final String listenerName, final String clientId, final List<Node> aliveBrokers) {
        final String explicitAZ = normalizeAZ(
            ClientAZExtractor.getClientAZ(clientId, () -> computeKnownRacks(aliveBrokers)));
        if (explicitAZ != null) {
            return explicitAZ;
        }
        if (listenerName != null) {
            // Config keys are normalized upper-case; ListenerName.value() is already normalized.
            final String az = clientAzListenerMap.get(listenerName);
            if (az != null) {
                return az;
            }
        }
        return null;
    }

    private Set<String> computeKnownRacks(final List<Node> aliveBrokers) {
        final Set<String> racks = new HashSet<>(clientAzListenerMap.values());
        aliveBrokers.stream()
            .map(Node::rack)
            .filter(r -> r != null && !r.isBlank())
            .forEach(racks::add);
        return racks;
    }

    /**
     * Record cross-AZ routing only when we can confirm the leader is in a different AZ.
     * If client AZ is unknown or the leader's rack is unset, we cannot confirm cross-AZ.
     */
    private void recordCrossAzIfConfirmed(final String clientAZ, final int leaderId, final Map<Integer, String> brokerRacks) {
        if (clientAZ == null) return;
        final String leaderRack = brokerRacks.get(leaderId);
        if (leaderRack != null && !leaderRack.isBlank() && !leaderRack.equals(clientAZ)) {
            metrics.recordCrossAzRouting();
        }
    }

    private static List<Integer> filterByAZ(
        final Map<Integer, String> brokerRacks,
        final List<Integer> brokerIds,
        final String az
    ) {
        if (az == null) {
            return Collections.emptyList();
        }
        return brokerIds.stream()
            .filter(id -> Objects.equals(brokerRacks.get(id), az))
            .toList();
    }

    private static int selectByHash(final Uuid topicId, final int partitionIndex, final List<Integer> candidates) {
        final List<Integer> sorted = candidates.stream().sorted().toList();
        final byte[] input = String.format("%s-%s", topicId, partitionIndex).getBytes(StandardCharsets.UTF_8);
        final int hash = Utils.murmur2(input);
        return sorted.get(Utils.toPositive(hash) % sorted.size());
    }

    @Override
    public void close() throws IOException {
        metrics.close();
    }

    /**
     * Result of leader selection for managed replicas.
     */
    private record LeaderSelectionResult(int leaderId, List<Integer> aliveReplicas, List<Integer> offlineReplicas) {}
}
