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

import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;

import java.util.List;
import java.util.Objects;

/**
 * Chooses the brokers that Kafka advertises for diskless partitions.
 *
 * <p>No KRaft leader serves a diskless partition, so each engine decides which brokers a client
 * contacts. The choice encodes the engine's write model: an engine that accepts concurrent writers
 * can spread clients across brokers, while a single-writer engine must route every client of a
 * partition to its owner. Kafka rewrites the Metadata and DescribeTopicPartitions responses with
 * the result and keeps KRaft leader epochs unchanged.
 */
@FunctionalInterface
public interface PartitionPlacement {
    /**
     * A diskless partition with its KRaft replica assignment.
     *
     * @param replicas the assigned replicas; more than one means Kafka manages local replicas
     * @param remoteStorageEnabled whether the topic keeps a Kafka log tier that only replicas can read
     */
    record PartitionAssignment(Uuid topicId, int partition, List<Integer> replicas, boolean remoteStorageEnabled) {
        public PartitionAssignment {
            Objects.requireNonNull(topicId, "topicId");
            replicas = List.copyOf(replicas);
        }
    }

    /**
     * The routing that Kafka advertises for one partition. A negative leader means that no broker
     * can serve the partition; Kafka then keeps the partition error from KRaft.
     */
    record Placement(int leaderId, List<Integer> replicas, List<Integer> isr, List<Integer> offlineReplicas) {
        public Placement {
            replicas = List.copyOf(replicas);
            isr = List.copyOf(isr);
            offlineReplicas = List.copyOf(offlineReplicas);
        }

        public boolean available() {
            return leaderId >= 0;
        }
    }

    /**
     * Returns one placement per assignment, in order. Kafka calls it on the request thread for each
     * Metadata and DescribeTopicPartitions response, so it must not block.
     *
     * @param client the requesting client; the engine derives any client zone from it
     * @param aliveBrokers the unfenced brokers that expose the client's listener
     */
    List<Placement> place(DisklessRequestContext client, List<Node> aliveBrokers, List<PartitionAssignment> partitions);
}
