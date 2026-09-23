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
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.server.util.Scheduler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;

import io.aiven.inkless.common.SharedState;
import io.aiven.inkless.consume.FetchHandler;
import io.aiven.inkless.consume.FetchOffsetHandler;
import io.aiven.inkless.control_plane.FindBatchRequest;
import io.aiven.inkless.control_plane.FindBatchResponse;
import io.aiven.inkless.delete.DeleteRecordsInterceptor;
import io.aiven.inkless.delete.FileCleaner;
import io.aiven.inkless.delete.RetentionEnforcer;
import io.aiven.inkless.delete.TopicPurger;
import io.aiven.inkless.produce.AppendHandler;

/** Adapts the existing handlers without changing their storage or scheduling behavior. */
public final class InklessDisklessEngine implements DisklessEngine {
    private AppendHandler appendHandler;
    private FetchHandler fetchHandler;
    private FetchOffsetHandler offsetHandler;

    private SharedState sharedState;
    private InklessTieredStorage tieredStorage;
    private DeleteRecordsInterceptor deleteRecords;
    private RetentionEnforcer retention;
    private FileCleaner cleaner;
    private TopicPurger purger;
    private final List<ScheduledFuture<?>> tasks = new ArrayList<>();
    private boolean started;
    private boolean closed;

    public InklessDisklessEngine(SharedState sharedState, Optional<ConsolidationConfig> consolidation) {
        this.sharedState = sharedState;
        try {
            this.appendHandler = new AppendHandler(sharedState);
            this.fetchHandler = new FetchHandler(sharedState);
            this.offsetHandler = new FetchOffsetHandler(sharedState);
            this.tieredStorage = new InklessTieredStorage(sharedState, consolidation);
            this.deleteRecords = new DeleteRecordsInterceptor(sharedState);
            this.retention = new RetentionEnforcer(sharedState);
            this.cleaner = new FileCleaner(sharedState);
            this.purger = new TopicPurger(sharedState);
        } catch (RuntimeException | Error failure) {
            try {
                closeComponents();
            } catch (IOException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            // Ownership of SharedState transfers only after construction succeeds.
            throw failure;
        }
    }

    public InklessDisklessEngine(SharedState sharedState) {
        this(sharedState, Optional.empty());
    }

    public record ConsolidationConfig(int metadataThreads, int dataThreads,
                                      int requestRateLimit, int maxBatchesPerPartition) { }

    @Override
    public synchronized void start(Scheduler scheduler, long initialDelayMs) {
        if (started || closed) {
            throw new IllegalStateException("Engine already started or closed");
        }
        started = true;
        tasks.add(scheduler.schedule("inkless-retention-enforcer", retention, initialDelayMs, 500L));
        schedule(scheduler, "inkless-file-cleaner", cleaner, sharedState.config().fileCleanerInterval().toMillis());
        schedule(scheduler, "inkless-topic-purger", purger, sharedState.config().topicPurgerInterval().toMillis());
        // Waiting for the broker's default task delay would leave EARLIEST stale after startup.
        schedule(scheduler, "inkless-cross-tier-log-start-reporter", sharedState.crossTierLogStartReporter(),
            sharedState.config().crossTierLogStartReportInterval().toMillis());
    }

    private void schedule(Scheduler scheduler, String name, Runnable task, long intervalMs) {
        tasks.add(scheduler.schedule(name, task, intervalMs, intervalMs));
    }

    @Override
    public boolean supportsDeleteRecords() {
        return true;
    }

    @Override
    public CompletableFuture<Map<TopicPartition, DeleteRecordsPartitionResult>> deleteRecords(
        Map<TopicPartition, Long> offsets) {
        var result = new CompletableFuture<Map<TopicPartition, DeleteRecordsPartitionResult>>();
        try {
            if (!deleteRecords.intercept(offsets, result::complete)) {
                result.completeExceptionally(new IllegalArgumentException("Expected diskless partitions"));
            }
        } catch (Exception e) {
            result.completeExceptionally(e);
        }
        return result;
    }

    @Override
    public Optional<TieredStorage> tieredStorage() {
        return Optional.ofNullable(tieredStorage);
    }

    @Override
    public Optional<List<FetchAvailability>> probeFetch(List<FetchProbe> requests) {
        List<FindBatchResponse> responses;
        if (!sharedState.isBatchCoordinateCacheEnabled()) {
            responses = sharedState.controlPlane().findBatches(requests.stream()
                .map(r -> new FindBatchRequest(r.partition(), r.offset(), r.maxBytes())).toList(),
                Integer.MAX_VALUE, sharedState.config().maxBatchesPerPartitionToFind());
        } else {
            responses = requests.stream().map(request -> {
                var fragment = sharedState.batchCoordinateCache().get(request.partition(), request.offset());
                if (fragment == null) {
                    return FindBatchResponse.success(List.of(), FindBatchResponse.UNKNOWN_OFFSET,
                        FindBatchResponse.UNKNOWN_OFFSET);
                }
                return FindBatchResponse.success(fragment.batches().stream()
                    .map(batch -> batch.batchInfo(request.partition())).toList(),
                    fragment.logStartOffset(), fragment.highWaterMark());
            }).toList();
        }
        var result = new ArrayList<FetchAvailability>();
        for (int i = 0; i < requests.size(); i++) {
            var request = requests.get(i);
            var response = responses.get(i);
            result.add(new FetchAvailability(request.partition(), response.errors(),
                response.errors() == Errors.NONE && !response.batches().isEmpty(),
                response.highWatermark(), response.estimatedByteSize(request.offset())));
        }
        return Optional.of(result);
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
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        tasks.forEach(task -> task.cancel(false));
        Utils.closeAll(this::closeComponents, sharedState);
    }

    private void closeComponents() throws IOException {
        Utils.closeAll(appendHandler, fetchHandler, offsetHandler, tieredStorage,
            retention, cleaner, purger, deleteRecords);
    }
}
