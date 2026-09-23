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
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import io.aiven.inkless.common.SharedState;
import io.aiven.inkless.consume.FetchHandler;
import io.aiven.inkless.consume.FetchOffsetHandler;
import io.aiven.inkless.produce.AppendHandler;

/** Adapts the existing handlers without changing their storage or scheduling behavior. */
public final class InklessDisklessEngine implements DisklessEngine {
    private final AppendHandler appendHandler;
    private final FetchHandler fetchHandler;
    private final FetchOffsetHandler offsetHandler;

    public InklessDisklessEngine(SharedState sharedState) {
        this(new AppendHandler(sharedState), new FetchHandler(sharedState), new FetchOffsetHandler(sharedState));
    }

    InklessDisklessEngine(AppendHandler appendHandler, FetchHandler fetchHandler, FetchOffsetHandler offsetHandler) {
        this.appendHandler = appendHandler;
        this.fetchHandler = fetchHandler;
        this.offsetHandler = offsetHandler;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // SharedState already configures the native handlers.
    }

    @Override
    public CompletableFuture<Map<TopicIdPartition, PartitionResponse>> append(
        Map<TopicIdPartition, MemoryRecords> records, RequestLocal requestLocal) {
        return appendHandler.handle(records, requestLocal);
    }

    @Override
    public CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> fetch(
        FetchParams params, Map<TopicIdPartition, FetchRequest.PartitionData> partitions) {
        return fetchHandler.handle(params, partitions);
    }

    @Override
    public OffsetJob createOffsetJob() {
        return offsetHandler.createJob();
    }

    @Override
    public void close() throws IOException {
        Utils.closeAll(appendHandler, fetchHandler, offsetHandler);
    }
}
