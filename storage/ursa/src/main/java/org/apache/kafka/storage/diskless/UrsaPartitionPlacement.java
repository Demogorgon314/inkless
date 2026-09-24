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
import org.apache.kafka.common.utils.Utils;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import io.aiven.inkless.engine.DisklessRequestContext;
import io.aiven.inkless.engine.PartitionPlacement;

/**
 * Routes every client of a partition to one owner broker, as UFK's broker selector does without a
 * client zone. Ursa leases each partition log to one writer and keeps producer state in a single
 * namespace, so spreading one partition across brokers would make writers fence each other.
 *
 * <p>Zone-local owners require zone-scoped producer state and owner reconciliation. Enable them
 * here together with those changes, not through client routing hints alone.
 */
final class UrsaPartitionPlacement implements PartitionPlacement {
    @Override
    public List<Placement> place(DisklessRequestContext client, List<Node> aliveBrokers,
                                 List<PartitionAssignment> partitions) {
        List<Node> brokers = aliveBrokers.stream().sorted(Comparator.comparingInt(Node::id)).toList();
        List<Placement> placements = new ArrayList<>(partitions.size());
        for (PartitionAssignment partition : partitions) {
            if (brokers.isEmpty()) {
                placements.add(new Placement(-1, partition.replicas(), List.of(), List.of()));
                continue;
            }
            byte[] key = (partition.topicId() + "-" + partition.partition()).getBytes(StandardCharsets.UTF_8);
            int owner = brokers.get(Math.floorMod(Utils.murmur2(key), brokers.size())).id();
            placements.add(new Placement(owner, List.of(owner), List.of(owner), List.of()));
        }
        return placements;
    }
}
