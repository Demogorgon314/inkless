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

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.storage.internals.log.OffsetResultHolder.FileRecordsOrError;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

/**
 * Experimental broker data-plane boundary. The engine owns record validation, offsets,
 * producer state, and persistence; Kafka owns authorization and classic/diskless routing.
 * This interface is not a complete storage-provider or stable public API.
 */
public interface DisklessEngine extends Configurable, Closeable {
    /**
     * Completes after the engine commits the records. Uses RequestLocal only on the calling
     * thread; asynchronous work must own any buffers it retains beyond the call.
     */
    CompletableFuture<Map<TopicIdPartition, PartitionResponse>> append(
        Map<TopicIdPartition, MemoryRecords> records, RequestLocal requestLocal);

    /**
     * Preserves request iteration order when spending the shared fetch byte budget.
     * Returns a result for every requested partition, including partition-level failures.
     */
    CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> fetch(
        FetchParams params, Map<TopicIdPartition, FetchRequest.PartitionData> partitions);

    OffsetJob createOffsetJob();

    /**
     * Batches lookups before start and exposes cancellation to Kafka's ListOffsets purgatory.
     * Each request, including a delayed hybrid-read fallback, needs a fresh job.
     * This shape preserves the existing broker integration for the proof of concept.
     */
    interface OffsetJob {
        boolean mustHandle(String topic);

        CompletableFuture<FileRecordsOrError> add(TopicPartition partition, ListOffsetsPartition request);

        Future<Void> cancelHandler();

        void start();
    }
}
