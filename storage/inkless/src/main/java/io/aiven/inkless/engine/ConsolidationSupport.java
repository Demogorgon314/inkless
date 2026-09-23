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
import org.apache.kafka.common.protocol.Errors;

import java.util.Map;
import java.util.OptionalLong;

/**
 * Kafka owns local-log coordination and retries; the engine owns external metadata and caches.
 * Metadata methods retain the native synchronous contract; callers must not treat them as nonblocking.
 * Returned services are owned and closed by the engine, never by their callers.
 */
public interface ConsolidationSupport extends Fetcher {
    /** A nonnegative offset is usable only with NONE; a negative offset means no value is available. */
    record OffsetResult(Errors error, long offset) { }

    /**
     * Returns only the remote start reported by the local-log leader. Never substitutes the
     * diskless prune frontier: Kafka uses this value to decide which remote data it can reclaim.
     */
    OptionalLong remoteLogStartOffset(TopicIdPartition partition);

    /** Returns the cross-tier logical earliest offset, independently of this broker's local log. */
    OptionalLong earliestOffset(TopicIdPartition partition);

    /** Advances logical earliest offsets monotonically; does not physically delete remote-tier data. */
    Map<TopicIdPartition, OffsetResult> advanceEarliestOffsets(Map<TopicIdPartition, Long> offsets);

    /** Queues the leader's remote-start report for engine-owned persistence. */
    void reportRemoteLogStartOffset(TopicPartition partition, long offset);

    /** Prunes through Kafka's inclusive safe tiered offsets; results contain the new diskless starts. */
    Map<TopicIdPartition, OffsetResult> prune(Map<TopicIdPartition, Long> highestTieredOffsets);

}
