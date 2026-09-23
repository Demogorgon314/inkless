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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsPartitionResult;
import org.apache.kafka.common.protocol.Errors;

import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

/**
 * Controls logical retention separately from reclamation of records copied to Kafka's log tiers.
 * Cross-tier operations require KAFKA_LOG_TIERING and retain their synchronous contract.
 * Reclamation must not remove the last readable copy or change the logical earliest offset.
 */
public interface LogRetention {
    /** Requires DELETE_RECORDS. Returns one result per partition, including failures. */
    default CompletableFuture<Map<TopicPartition, DeleteRecordsPartitionResult>> deleteRecords(
        Map<TopicPartition, Long> offsets) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("DeleteRecords is not supported"));
    }

    /** A nonnegative offset is usable only with NONE; a negative offset means no value is available. */
    record OffsetResult(Errors error, long offset) { }

    /**
     * Returns only the remote start reported by the local-log leader. Never substitutes the
     * diskless prune frontier: Kafka uses this value to decide which remote data it can reclaim.
     */
    default OptionalLong remoteLogStartOffset(TopicIdPartition partition) {
        throw new UnsupportedOperationException("Kafka log tiering is not supported");
    }

    /** Returns the cross-tier logical earliest offset, independently of this broker's local log. */
    default OptionalLong earliestOffset(TopicIdPartition partition) {
        throw new UnsupportedOperationException("Kafka log tiering is not supported");
    }

    /** Advances logical earliest offsets monotonically; does not physically delete remote-tier data. */
    default Map<TopicIdPartition, OffsetResult> advanceEarliestOffsets(Map<TopicIdPartition, Long> offsets) {
        throw new UnsupportedOperationException("Kafka log tiering is not supported");
    }

    /** Queues the leader's remote-start report for engine-owned persistence. */
    default void reportRemoteLogStartOffset(TopicPartition partition, long offset) {
        throw new UnsupportedOperationException("Kafka log tiering is not supported");
    }

    /** Reclaims through Kafka's inclusive safe tiered offsets; results contain the new engine-resident starts. */
    default Map<TopicIdPartition, OffsetResult> reclaimReplicatedRecords(Map<TopicIdPartition, Long> highestTieredOffsets) {
        throw new UnsupportedOperationException("Kafka log tiering is not supported");
    }

}
