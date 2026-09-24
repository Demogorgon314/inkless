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

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Advances the logical start offset of diskless partitions for DeleteRecords requests. */
@FunctionalInterface
public interface RecordDeletion {
    /** A nonnegative low watermark is meaningful only with NONE. */
    record DeleteRecordsResult(Errors error, long lowWatermark) { }

    /**
     * Returns one result per requested partition, including failures. Kafka resolves topic IDs and
     * runs any local-log deletion before calling this method.
     */
    CompletableFuture<Map<TopicIdPartition, DeleteRecordsResult>> deleteRecords(Map<TopicIdPartition, Long> offsets);
}
