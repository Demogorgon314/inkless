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
package kafka.server

import io.aiven.inkless.engine.{LogTiering, RecordDeletion}
import kafka.cluster.Partition
import kafka.server.metadata.DisklessTopicView
import kafka.utils.Logging
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsPartitionResult
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{DeleteRecordsRequest, DeleteRecordsResponse}
import org.apache.kafka.metadata.PartitionRegistration

import java.util
import scala.collection.{Map, mutable}
import scala.jdk.CollectionConverters._

/**
 * Splits DeleteRecords between the local log and the diskless engine, runs the engine leg, and
 * reports the cross-tier earliest of consolidating partitions. A partition switched from classic,
 * or one that consolidates into a local tier, deletes from both; Kafka runs the local leg first.
 */
final class DisklessDeleteRecords(topicView: DisklessTopicView,
                                  recordDeletion: Option[RecordDeletion],
                                  logTiering: Option[LogTiering],
                                  partitionOrError: TopicPartition => Either[Errors, Partition],
                                  consolidationEnabled: Boolean) extends Logging {

  def split(offsetPerPartition: Map[TopicPartition, Long]): DisklessDeleteRecordsPlan = {
    val disklessStartOffsetPerPartition = offsetPerPartition.keys.flatMap { topicPartition =>
      if (topicView.isDisklessTopic(topicPartition.topic)) {
        Some(topicPartition -> topicView.getClassicToDisklessStartOffset(topicPartition))
      } else {
        None
      }
    }.toMap
    val disklessDeleteRecordsRequested = disklessStartOffsetPerPartition.nonEmpty

    val failedDisklessDeleteRecords = if (disklessDeleteRecordsRequested && recordDeletion.isEmpty) {
      error(s"Cannot delete records from diskless partitions ${disklessStartOffsetPerPartition.keys.mkString(", ")}: the engine does not support DeleteRecords")
      disklessStartOffsetPerPartition.keys.map { topicPartition =>
        topicPartition -> new DeleteRecordsPartitionResult()
          .setPartitionIndex(topicPartition.partition)
          .setLowWatermark(DeleteRecordsResponse.INVALID_LOW_WATERMARK)
          .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code)
      }.toMap
    } else {
      Map.empty[TopicPartition, DeleteRecordsPartitionResult]
    }

    val localOffsetPerPartition = mutable.Map.empty[TopicPartition, Long]
    val disklessOffsetPerPartition = mutable.Map.empty[TopicPartition, Long]
    val hybridDisklessPartitions = mutable.Set.empty[TopicPartition]

    if (disklessDeleteRecordsRequested) {
      offsetPerPartition.filterNot { case (topicPartition, _) =>
        failedDisklessDeleteRecords.contains(topicPartition)
      }.foreach { case (topicPartition, requestedOffset) =>
        disklessStartOffsetPerPartition.get(topicPartition) match {
          case Some(classicToDisklessStartOffset) if classicToDisklessStartOffset >= 0 =>
            // partition switched from classic to diskless
            val needsDisklessDelete = requestedOffset == DeleteRecordsRequest.HIGH_WATERMARK ||
              requestedOffset > classicToDisklessStartOffset
            val localOffset = if (needsDisklessDelete && requestedOffset != DeleteRecordsRequest.HIGH_WATERMARK) {
              classicToDisklessStartOffset
            } else {
              requestedOffset
            }
            localOffsetPerPartition += topicPartition -> localOffset
            if (needsDisklessDelete) {
              disklessOffsetPerPartition += topicPartition -> requestedOffset
              hybridDisklessPartitions += topicPartition
            }
          case Some(PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING) =>
            // partition not switched yet to diskless: data is only available in local log
            localOffsetPerPartition += topicPartition -> requestedOffset
          case Some(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET)
            if consolidationEnabled &&
              topicView.isConsolidatingDisklessTopic(topicPartition.topic) =>
            partitionOrError(topicPartition) match {
              case Right(partition) =>
                partition.log match {
                  case Some(log) =>
                    // consolidating diskless partition with local data
                    val localLogEndOffset = log.logEndOffset
                    val needsDisklessDelete = requestedOffset == DeleteRecordsRequest.HIGH_WATERMARK ||
                      requestedOffset > localLogEndOffset
                    val localOffset = if (needsDisklessDelete && requestedOffset != DeleteRecordsRequest.HIGH_WATERMARK) {
                      localLogEndOffset
                    } else {
                      requestedOffset
                    }
                    localOffsetPerPartition += topicPartition -> localOffset
                    if (needsDisklessDelete) {
                      disklessOffsetPerPartition += topicPartition -> requestedOffset
                      hybridDisklessPartitions += topicPartition
                    }
                  case None =>
                    // consolidating partition has no local log, so only diskless data can be deleted
                    disklessOffsetPerPartition += topicPartition -> requestedOffset
                }
              case Left(error) =>
                // cannot inspect the local log, no local component to delete, treat as pure-diskless
                disklessOffsetPerPartition += topicPartition -> requestedOffset
                warn(s"Cannot find local partition for consolidating diskless topic " +
                  s"${topicPartition}, routing delete exclusively to diskless. Error: $error")
            }
          case Some(_) =>
            // pure-diskless partition
            disklessOffsetPerPartition += topicPartition -> requestedOffset
          case None =>
            // classic partition
            localOffsetPerPartition += topicPartition -> requestedOffset
        }
      }
    } else {
      // No diskless partitions are present, so keep the existing local delete path.
      localOffsetPerPartition ++= offsetPerPartition
    }

    new DisklessDeleteRecordsPlan(localOffsetPerPartition.toMap, disklessOffsetPerPartition.toMap,
      hybridDisklessPartitions.toSet, failedDisklessDeleteRecords)
  }

  /** Deletes through the engine and passes one result per requested partition to `respond`. */
  def deleteFromEngine(offsets: Map[TopicPartition, Long],
                       respond: Map[TopicPartition, DeleteRecordsPartitionResult] => Unit): Unit = {
    val deletion = recordDeletion.getOrElse(
      throw new IllegalStateException("The engine does not support DeleteRecords"))
    def partitionResult(tp: TopicPartition, error: Errors, lowWatermark: Long): DeleteRecordsPartitionResult =
      new DeleteRecordsPartitionResult().setPartitionIndex(tp.partition)
        .setLowWatermark(lowWatermark).setErrorCode(error.code)
    val requests = new util.LinkedHashMap[TopicIdPartition, java.lang.Long]()
    val unresolved = mutable.Map.empty[TopicPartition, DeleteRecordsPartitionResult]
    offsets.foreach { case (tp, offset) =>
      val topicId = topicView.getTopicId(tp.topic)
      if (topicId == null || topicId == Uuid.ZERO_UUID)
        unresolved += tp -> partitionResult(tp, Errors.UNKNOWN_TOPIC_OR_PARTITION, DeleteRecordsResponse.INVALID_LOW_WATERMARK)
      else
        requests.put(new TopicIdPartition(topicId, tp), offset)
    }
    deletion.deleteRecords(requests).whenComplete { (results, failure) =>
      val response = if (failure == null) {
        results.asScala.map { case (partition, result) =>
          partition.topicPartition -> partitionResult(partition.topicPartition, result.error, result.lowWatermark)
        }.toMap
      } else {
        requests.keySet.asScala.map { partition =>
          partition.topicPartition -> partitionResult(partition.topicPartition, Errors.forException(failure),
            DeleteRecordsResponse.INVALID_LOW_WATERMARK)
        }.toMap
      }
      respond(unresolved ++ response)
    }
  }

  /**
   * For every successfully-deleted consolidating diskless partition in `response`, advance the
   * control-plane cross-tier earliest (`remote_log_start_offset`) to the requested delete offset and
   * return a replacement result whose low watermark is the stored value. `EARLIEST` is
   * `COALESCE(remote_log_start_offset, log_start_offset)`, so reporting the just-advanced (non-null)
   * `remote_log_start_offset` keeps the DeleteRecords low watermark and a subsequent
   * `ListOffsets(EARLIEST)` in agreement on every broker.
   *
   * Physical deletion is unchanged: the diskless WAL objects are freed by `delete_records_v1` +
   * `FileCleaner`, and the remote-tier segments by the leader's `RemoteLogManager` (driven by the
   * leader's local `logStartOffset`); this method only moves the logical earliest pointer.
   *
   * Returns the replacement results keyed by partition (empty when nothing applies). The control-plane
   * call is synchronous; `DeleteRecords` is an infrequent admin operation.
   */
  def advanceCrossTierEarliest(
    response: Map[TopicPartition, DeleteRecordsPartitionResult],
    offsetPerPartition: Map[TopicPartition, Long]
  ): Map[TopicPartition, DeleteRecordsPartitionResult] = {
    val storage = logTiering.orNull
    if (storage == null) {
      return Map.empty
    }

    // Convert each delete offset: use the requested offset, or the leg's returned low watermark when
    // the request was HIGH_WATERMARK (which always reaches the WAL, so the diskless leg ran).
    val requests = new java.util.LinkedHashMap[TopicIdPartition, java.lang.Long]()
    val partitionsInOrder = mutable.ArrayBuffer.empty[TopicPartition]
    response.foreach { case (topicPartition, result) =>
      // Non-consolidating partitions and errors will be skipped here as they're handled in the caller
      // finalizeCrossTierAndRespond by returning their original response.
      if (result.errorCode == Errors.NONE.code &&
        topicView.isConsolidatingDisklessTopic(topicPartition.topic)) {
        val requested = offsetPerPartition.getOrElse(topicPartition, DeleteRecordsRequest.HIGH_WATERMARK)
        val convertedOffset = if (requested == DeleteRecordsRequest.HIGH_WATERMARK) result.lowWatermark else requested
        val topicId = topicView.getTopicId(topicPartition.topic)
        if (convertedOffset >= 0 && topicId != null && !topicId.equals(Uuid.ZERO_UUID)) {
          partitionsInOrder += topicPartition
          requests.put(new TopicIdPartition(topicId, topicPartition), convertedOffset)
        }
      }
    }

    if (requests.isEmpty) {
      return Map.empty
    }

    try {
      val responses = storage.advanceEarliestOffsets(requests)
      val replacements = mutable.Map.empty[TopicPartition, DeleteRecordsPartitionResult]
      responses.asScala.foreach { case (partition, result) =>
        if (result.error() == Errors.NONE && result.offset() >= 0) {
          replacements += partition.topicPartition() -> new DeleteRecordsPartitionResult()
            .setPartitionIndex(partition.partition()).setLowWatermark(result.offset()).setErrorCode(Errors.NONE.code)
        }
      }
      replacements.toMap
    } catch {
      case e: Exception =>
        // Catch only Exception, not Throwable: control-plane failures are ControlPlaneException (a
        // RuntimeException), but Errors (e.g. OutOfMemoryError) must propagate rather than fail open.
        // Reporting failure must not fail the delete (the data is already deleted); leave the per-leg
        // low watermark in place and let the RLM's own report reconcile the control plane later.
        error(s"Failed to advance cross-tier log start offset for ${partitionsInOrder.mkString(", ")}", e)
        Map.empty
    }
  }
}

/** The legs of one DeleteRecords request. Hybrid partitions delete from both legs. */
final class DisklessDeleteRecordsPlan(val local: Map[TopicPartition, Long],
                                      val diskless: Map[TopicPartition, Long],
                                      hybrid: Set[TopicPartition],
                                      val failed: Map[TopicPartition, DeleteRecordsPartitionResult]) {
  /** Returns the engine leg, without hybrid partitions whose local leg failed. */
  def disklessAfterLocal(localResponse: Map[TopicPartition, DeleteRecordsPartitionResult]): Map[TopicPartition, Long] =
    diskless.filterNot { case (topicPartition, _) =>
      hybrid.contains(topicPartition) && localResponse.get(topicPartition).exists(_.errorCode != Errors.NONE.code)
    }
}
