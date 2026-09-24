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
package org.apache.kafka.storage.diskless;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.IntStream;

import io.aiven.inkless.engine.DisklessRequestContext;
import io.aiven.inkless.engine.PartitionPlacement.PartitionAssignment;
import io.aiven.inkless.engine.PartitionPlacement.Placement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UrsaPartitionPlacementTest {
    private static final Uuid TOPIC_ID = new Uuid(123, 456);
    private static final List<Node> BROKERS = List.of(
        new Node(3, "b3", 9092, "az-b"), new Node(1, "b1", 9092, "az-a"), new Node(2, "b2", 9092, "az-a"));

    private final UrsaPartitionPlacement placement = new UrsaPartitionPlacement();

    private static List<PartitionAssignment> partitions(int count) {
        return IntStream.range(0, count)
            .mapToObj(p -> new PartitionAssignment(TOPIC_ID, p, List.of(1), false)).toList();
    }

    @Test
    void routesEveryClientOfAPartitionToOneOwner() {
        var assignments = partitions(16);
        var reference = placement.place(new DisklessRequestContext(null, null), BROKERS, assignments);
        for (var client : List.of(
            new DisklessRequestContext("diskless_az=az-a", "PLAINTEXT"),
            new DisklessRequestContext("zone_id=az-b", "EXTERNAL"),
            new DisklessRequestContext("", ""))) {
            assertEquals(reference, placement.place(client, BROKERS, assignments));
        }
        // Broker order in the metadata cache must not change the owner.
        var reordered = List.of(BROKERS.get(2), BROKERS.get(0), BROKERS.get(1));
        assertEquals(reference, placement.place(new DisklessRequestContext(null, null), reordered, assignments));
    }

    @Test
    void advertisesOnlyTheOwnerAndSpreadsPartitions() {
        var placements = placement.place(new DisklessRequestContext(null, null), BROKERS, partitions(16));
        for (Placement result : placements) {
            assertTrue(result.available());
            assertEquals(List.of(result.leaderId()), result.replicas());
            assertEquals(List.of(result.leaderId()), result.isr());
            assertTrue(result.offlineReplicas().isEmpty());
        }
        assertTrue(placements.stream().map(Placement::leaderId).distinct().count() > 1,
            "Owners must be spread across brokers");
    }

    @Test
    void reportsUnavailableWithoutAliveBrokers() {
        var result = placement.place(new DisklessRequestContext(null, null), List.of(), partitions(1)).get(0);
        assertFalse(result.available());
        assertEquals(List.of(1), result.replicas());
    }
}
