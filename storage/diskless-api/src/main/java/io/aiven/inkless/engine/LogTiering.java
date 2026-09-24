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
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;

import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

/**
 * Copies diskless records into Kafka-managed logs and tracks the offsets that span both tiers.
 *
 * <p>Kafka copies records, reports the remote start offset, and decides the safe durable-copy
 * boundary. The engine reclaims its copies only up to that boundary. Reclamation never changes the
 * logical earliest offset or removes the last readable copy. Offset methods retain their
 * synchronous contract.
 */
public interface LogTiering {
    /** A nonnegative offset is usable only with NONE; a negative offset means no value is available. */
    record OffsetResult(Errors error, long offset) { }

    /**
     * Reads records for copying into Kafka's log tiers using the fetch result and ownership contract.
     * Implementations may isolate this background work from client fetches. Kafka calls it only
     * while it runs consolidation fetchers.
     */
    CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> fetchForReplication(
        FetchParams params, Map<TopicIdPartition, FetchRequest.PartitionData> partitions);

    /**
     * Returns only the remote start reported by the local-log leader. Never substitutes the start of
     * the engine-resident records: Kafka uses this value to decide which remote data it can reclaim.
     */
    OptionalLong remoteLogStartOffset(TopicIdPartition partition);

    /** Returns the cross-tier logical earliest offset, independently of this broker's local log. */
    OptionalLong earliestOffset(TopicIdPartition partition);

    /** Advances logical earliest offsets monotonically; does not physically delete remote-tier data. */
    Map<TopicIdPartition, OffsetResult> advanceEarliestOffsets(Map<TopicIdPartition, Long> offsets);

    /** Queues the leader's remote-start report for engine-owned persistence. */
    void reportRemoteLogStartOffset(TopicIdPartition partition, long offset);

    /** Returns how often Kafka offers its safe tiered offsets to {@link #reclaimReplicatedRecords}. */
    long reclaimIntervalMs();

    /** Reclaims through Kafka's inclusive safe tiered offsets; results contain the new engine-resident starts. */
    Map<TopicIdPartition, OffsetResult> reclaimReplicatedRecords(Map<TopicIdPartition, Long> highestTieredOffsets);
}
