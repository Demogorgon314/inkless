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
package io.aiven.inkless.engine.builtin;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsPartitionResult;
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.requests.DeleteRecordsResponse;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.server.util.Scheduler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
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
import io.aiven.inkless.engine.DisklessEngine;
import io.aiven.inkless.engine.DisklessRequestContext;
import io.aiven.inkless.engine.FetchProbing;
import io.aiven.inkless.engine.LogTiering;
import io.aiven.inkless.engine.LogTransition;
import io.aiven.inkless.engine.RecordDeletion;
import io.aiven.inkless.produce.AppendHandler;

/** Adapts the existing handlers without changing their storage or scheduling behavior. */
public final class InklessDisklessEngine implements DisklessEngine, FetchProbing, RecordDeletion {
    private AppendHandler appendHandler;
    private FetchHandler fetchHandler;
    private FetchOffsetHandler offsetHandler;

    private SharedState sharedState;
    private Scheduler scheduler;
    private long initialTaskDelayMs;
    private InklessLogTiering logTiering;
    private LogTransition logTransition;
    private DeleteRecordsInterceptor deleteRecords;
    private RetentionEnforcer retention;
    private FileCleaner cleaner;
    private TopicPurger purger;
    private final List<ScheduledFuture<?>> tasks = new ArrayList<>();
    private boolean started;
    private boolean closed;

    /** Takes ownership of the shared state only when construction succeeds. */
    public InklessDisklessEngine(SharedState sharedState,
                                 Optional<InklessStorageProvider.ConsolidationConfig> consolidation,
                                 Scheduler scheduler,
                                 long initialTaskDelayMs) {
        this.sharedState = sharedState;
        this.scheduler = scheduler;
        this.initialTaskDelayMs = initialTaskDelayMs;
        try {
            this.appendHandler = new AppendHandler(sharedState);
            this.fetchHandler = new FetchHandler(sharedState);
            this.offsetHandler = new FetchOffsetHandler(sharedState);
            this.logTiering = new InklessLogTiering(sharedState, consolidation);
            this.logTransition = new InklessLogTransition(sharedState.controlPlane());
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
            throw failure;
        }
    }

    InklessDisklessEngine(AppendHandler appendHandler, FetchHandler fetchHandler, FetchOffsetHandler offsetHandler) {
        this.appendHandler = appendHandler;
        this.fetchHandler = fetchHandler;
        this.offsetHandler = offsetHandler;
    }

    @Override
    public synchronized void start() {
        if (started || closed) {
            throw new IllegalStateException("Engine already started or closed");
        }
        started = true;
        tasks.add(scheduler.schedule("inkless-retention-enforcer", retention, initialTaskDelayMs, 500L));
        schedule("inkless-file-cleaner", cleaner, sharedState.config().fileCleanerInterval().toMillis());
        schedule("inkless-topic-purger", purger, sharedState.config().topicPurgerInterval().toMillis());
        // Waiting for the broker's default task delay would leave EARLIEST stale after startup.
        schedule("inkless-cross-tier-log-start-reporter", sharedState.crossTierLogStartReporter(),
            sharedState.config().crossTierLogStartReportInterval().toMillis());
    }

    private void schedule(String name, Runnable task, long intervalMs) {
        tasks.add(scheduler.schedule(name, task, intervalMs, intervalMs));
    }

    @Override
    public Optional<FetchProbing> fetchProbing() {
        return Optional.of(this);
    }

    @Override
    public Optional<RecordDeletion> recordDeletion() {
        return Optional.of(this);
    }

    @Override
    public Optional<LogTiering> logTiering() {
        return Optional.ofNullable(logTiering);
    }

    @Override
    public Optional<LogTransition> logTransition() {
        return Optional.ofNullable(logTransition);
    }

    @Override
    public CompletableFuture<Map<TopicIdPartition, DeleteRecordsResult>> deleteRecords(Map<TopicIdPartition, Long> offsets) {
        var byTopicPartition = new LinkedHashMap<TopicPartition, TopicIdPartition>();
        var nativeOffsets = new LinkedHashMap<TopicPartition, Long>();
        offsets.forEach((partition, offset) -> {
            byTopicPartition.put(partition.topicPartition(), partition);
            nativeOffsets.put(partition.topicPartition(), offset);
        });
        var nativeResult = new CompletableFuture<Map<TopicPartition, DeleteRecordsPartitionResult>>();
        try {
            if (!deleteRecords.intercept(nativeOffsets, nativeResult::complete)) {
                nativeResult.completeExceptionally(new IllegalArgumentException("Expected diskless partitions"));
            }
        } catch (Exception e) {
            nativeResult.completeExceptionally(e);
        }
        return nativeResult.thenApply(results -> {
            var converted = new LinkedHashMap<TopicIdPartition, DeleteRecordsResult>();
            byTopicPartition.forEach((topicPartition, partition) -> {
                var result = results.get(topicPartition);
                converted.put(partition, result == null
                    ? new DeleteRecordsResult(Errors.UNKNOWN_SERVER_ERROR, DeleteRecordsResponse.INVALID_LOW_WATERMARK)
                    : new DeleteRecordsResult(Errors.forCode(result.errorCode()), result.lowWatermark()));
            });
            return converted;
        });
    }

    @Override
    public List<FetchAvailability> probeFetch(List<FetchProbe> requests) {
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
        return result;
    }

    @Override
    public CompletableFuture<Map<TopicIdPartition, PartitionResponse>> append(
        Map<TopicIdPartition, MemoryRecords> records, RequestLocal requestLocal, DisklessRequestContext requestContext) {
        return appendHandler.handle(records, requestLocal);
    }

    @Override
    public CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> fetch(
        FetchParams params, Map<TopicIdPartition, FetchRequest.PartitionData> partitions) {
        return fetchHandler.handle(params, partitions);
    }

    @Override
    public CompletableFuture<Map<TopicIdPartition, ListOffsetsResult>> listOffsets(
        Map<TopicIdPartition, ListOffsetsSpec> requests) {
        var job = offsetHandler.createJob();
        var nativeRequests = new LinkedHashMap<TopicIdPartition, ListOffsetsPartition>();
        var results = new LinkedHashMap<TopicIdPartition, CompletableFuture<ListOffsetsResult>>();
        requests.forEach((partition, spec) -> {
            var request = new ListOffsetsPartition().setPartitionIndex(partition.partition())
                .setTimestamp(spec.timestamp()).setCurrentLeaderEpoch(spec.currentLeaderEpoch().orElse(-1));
            nativeRequests.put(partition, request);
            results.put(partition, job.add(partition).thenApply(result -> {
                if (result.exception().isPresent()) {
                    return new ListOffsetsResult(Errors.forException(result.exception().get()), -1L, -1L, Optional.empty());
                }
                return result.timestampAndOffset().map(offset -> new ListOffsetsResult(
                    Errors.NONE, offset.timestamp, offset.offset, offset.leaderEpoch))
                    .orElseGet(() -> new ListOffsetsResult(Errors.NONE, -1L, -1L, Optional.empty()));
            }));
        });
        var completed = CompletableFuture.allOf(results.values().toArray(CompletableFuture[]::new))
            .thenApply(ignored -> {
                Map<TopicIdPartition, ListOffsetsResult> response = new LinkedHashMap<>();
                results.forEach((partition, result) -> response.put(partition, result.join()));
                return response;
            });
        completed.whenComplete((ignored, failure) -> {
            if (completed.isCancelled()) {
                job.cancelHandler().cancel(true);
            }
        });
        job.start(nativeRequests);
        return completed;
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
        Utils.closeAll(appendHandler, fetchHandler, offsetHandler, logTiering,
            retention, cleaner, purger, deleteRecords);
    }
}
