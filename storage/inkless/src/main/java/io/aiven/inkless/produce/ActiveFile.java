/*
 * Inkless
 * Copyright (C) 2024 - 2025 Aiven OY
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.aiven.inkless.produce;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.storage.internals.log.LogAppendInfo;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.LogValidator;
import org.apache.kafka.storage.internals.log.RecordValidationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.engine.DisklessTopicMetrics;
import io.aiven.inkless.engine.DisklessTopicMetrics.InvalidRecords;

/**
 * An active file.
 *
 * <p>This class is not thread-safe and is supposed to be protected with a lock at the call site.
 */
class ActiveFile {
    private static final Logger LOGGER = LoggerFactory.getLogger(ActiveFile.class);

    private final Time time;
    private Instant start;

    private int requestId = -1;

    private final BatchBuffer buffer;

    private final Map<Integer, Map<TopicIdPartition, MemoryRecords>> originalRequests = new HashMap<>();
    private final Map<Integer, CompletableFuture<Map<TopicIdPartition, PartitionResponse>>> awaitingFuturesByRequest = new HashMap<>();
    private final Map<Integer, Map<TopicIdPartition, PartitionResponse>> invalidBatchesByRequest = new HashMap<>();

    private final DisklessTopicMetrics metrics;
    private final LogValidator.MetricsRecorder validatorMetricsRecorder;

    ActiveFile(final Time time,
               final DisklessTopicMetrics metrics) {
        this.buffer = new BatchBuffer();
        this.time = time;
        this.metrics = metrics;
        this.validatorMetricsRecorder = validatorMetricsRecorder(metrics);
    }

    // For testing
    ActiveFile(final Time time, final Instant start) {
        this.buffer = new BatchBuffer();
        this.time = time;
        this.start = start;
        this.metrics = DisklessTopicMetrics.noop();
        this.validatorMetricsRecorder = validatorMetricsRecorder(metrics);
    }

    private static LogValidator.MetricsRecorder validatorMetricsRecorder(final DisklessTopicMetrics metrics) {
        return new LogValidator.MetricsRecorder() {
            @Override
            public void recordInvalidMagic() {
                metrics.markInvalidRecords(InvalidRecords.MAGIC);
            }

            @Override
            public void recordInvalidOffset() {
                metrics.markInvalidRecords(InvalidRecords.OFFSET_OR_SEQUENCE);
            }

            @Override
            public void recordInvalidSequence() {
                metrics.markInvalidRecords(InvalidRecords.OFFSET_OR_SEQUENCE);
            }

            @Override
            public void recordInvalidChecksums() {
                metrics.markInvalidRecords(InvalidRecords.CHECKSUM);
            }

            @Override
            public void recordNoKeyCompactedTopic() {
                metrics.markInvalidRecords(InvalidRecords.NO_KEY_COMPACTED_TOPIC);
            }
        };
    }

    // Eventually this could be refactored to be included within ReplicaManager as it shares a lot of similarities
    CompletableFuture<Map<TopicIdPartition, PartitionResponse>> add(
        final Map<TopicIdPartition, MemoryRecords> entriesPerPartition,
        final Map<String, LogConfig> topicConfigs,
        final RequestLocal requestLocal
    ) {
        Objects.requireNonNull(entriesPerPartition, "entriesPerPartition cannot be null");
        Objects.requireNonNull(topicConfigs, "topicConfigs cannot be null");
        Objects.requireNonNull(requestLocal, "requestLocal cannot be null");

        if (start == null) {
            start = TimeUtils.durationMeasurementNow(time);
        }

        requestId += 1;
        originalRequests.put(requestId, entriesPerPartition);
        final var invalidBatches = invalidBatchesByRequest.computeIfAbsent(requestId, id -> new HashMap<>());

        for (final var entry : entriesPerPartition.entrySet()) {
            final TopicIdPartition topicIdPartition = entry.getKey();

            final LogConfig config = topicConfigs.get(topicIdPartition.topic());
            if (config == null) {
                throw new IllegalArgumentException("Config not provided for topic " + topicIdPartition.topic());
            }

            // Similar to ReplicaManager#appendToLocalLog
            try {
                metrics.markProduceRequest(topicIdPartition.topic());

                final MemoryRecords records = entry.getValue();

                final LogAppendInfo appendInfo = UnifiedLog.validateAndAppendBatch(
                    time,
                    requestId,
                    topicIdPartition,
                    config,
                    records,
                    buffer,
                    invalidBatches,
                    requestLocal,
                    metrics,
                    validatorMetricsRecorder
                );

                // update stats for successfully appended bytes and messages as bytesInRate and messageInRate
                metrics.markBytesIn(topicIdPartition.topic(), records.sizeInBytes(), appendInfo.numMessages());
            // case e@ (_: UnknownTopicOrPartitionException |  // Handled earlier
            //          _: NotLeaderOrFollowerException | // Not relevant for diskless
            //          _: RecordTooLargeException |
            //          _: RecordBatchTooLargeException | // validation ignored during validation
            //          _: CorruptRecordException |
            //          _: KafkaStorageException) =>
            } catch (RecordTooLargeException | CorruptRecordException | KafkaStorageException e) {
                // NOTE: Failed produce requests metric is not incremented for known exceptions
                // it is supposed to indicate un-expected failures of a broker in handling a produce request
                invalidBatches.put(topicIdPartition, new PartitionResponse(Errors.forException(e)));
            } catch (RecordValidationException rve) {
                processFailedRecords(topicIdPartition.topicPartition(), rve.invalidException());
                invalidBatches.put(topicIdPartition,
                    new PartitionResponse(
                        Errors.forException(rve.invalidException()),
                        ProduceResponse.INVALID_OFFSET,
                        RecordBatch.NO_TIMESTAMP,
                        ProduceResponse.INVALID_OFFSET,
                        rve.recordErrors(),
                        rve.getMessage()
                    ));
            } catch (Throwable t) {
                processFailedRecords(topicIdPartition.topicPartition(), t);
                invalidBatches.put(topicIdPartition, new PartitionResponse(Errors.forException(t)));
            }
        }

        final CompletableFuture<Map<TopicIdPartition, PartitionResponse>> result = new CompletableFuture<>();
        awaitingFuturesByRequest.put(requestId, result);

        return result;
    }

    private void processFailedRecords(TopicPartition topicPartition, Throwable t) {
        metrics.markFailedProduceRequest(topicPartition.topic());
        if (t instanceof InvalidProducerEpochException) {
            LOGGER.info("Error processing append operation on partition {}", topicPartition, t);
        }
        LOGGER.error("Error processing append operation on partition {}", topicPartition, t);
    }

    boolean isEmpty() {
        return requestId < 0;
    }

    int size() {
        return buffer.totalSize();
    }

    ClosedFile close() {
        final BatchBuffer.CloseResult closeResult = buffer.close();
        return new ClosedFile(
            start,
            originalRequests,
            awaitingFuturesByRequest,
            closeResult.commitBatchRequests(),
            invalidBatchesByRequest,
            closeResult.data()
        );
    }
}
