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

import com.yammer.metrics.core.Meter
import kafka.cluster.Partition
import kafka.server.metadata.DisklessTopicView
import kafka.utils.Logging
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.common.message.FetchResponseData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.internal.MemoryRecords
import org.apache.kafka.common.requests.FetchRequest
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.utils.Time
import org.apache.kafka.metadata.PartitionRegistration
import org.apache.kafka.server.storage.log.{FetchParams, FetchPartitionData}
import org.apache.kafka.storage.internals.log.UnifiedLog

import java.util.{Optional, OptionalInt, OptionalLong}
import scala.collection.{Seq, mutable}
import scala.util.control.NonFatal

/**
 * Routes each partition of a fetch to the classic read path, the diskless engine, or an immediate
 * response. A partition switched from classic keeps a local-log prefix, and a consolidating
 * partition keeps a local tier, so the route of a diskless partition depends on its fetch offset.
 */
final class DisklessFetchPlanner(topicView: DisklessTopicView,
                                 partitionOrError: TopicPartition => Either[Errors, Partition],
                                 crossTierEarliestOffset: TopicPartition => OptionalLong,
                                 consolidationActive: String => Boolean,
                                 engineEnabled: Boolean,
                                 managedReplicasEnabled: Boolean,
                                 time: Time,
                                 disklessSwitchedPrefixMissingFetchRate: Meter,
                                 consolidationHighWatermarkLagFetchRate: Meter) extends Logging {

  def plan(params: FetchParams, fetchInfos: Seq[(TopicIdPartition, PartitionData)]): DisklessFetchPlan = {
    val disklessFetchInfos = new mutable.ArrayBuffer[(TopicIdPartition, PartitionData)]()
    val classicFetchInfos = new mutable.ArrayBuffer[(TopicIdPartition, PartitionData)]()
    val immediateFetchResponses = new mutable.ArrayBuffer[(TopicIdPartition, FetchPartitionData)]()
    // Legacy fetch versions (<13) omit the topic ID, so the request keys this partition with
    // Uuid.ZERO_UUID. maybeBackfillDisklessTopicId resolves the real topic ID for the diskless
    // read below, but the fetch session was built from the original (zero-UUID) request key. Track
    // the substitution here so respond() can restore the client's original partition identity,
    // keeping the fetch session lookup (FetchSession.scala) able to find its request data.
    val disklessTopicIdOverrides = new mutable.HashMap[TopicIdPartition, TopicIdPartition]()
    // Consolidating partitions served from local log that may need a diskless supplement.
    // Maps tp -> logEndOffset (the offset where the diskless supplement should start).
    val consolidatingLocalFetchSupplements = new mutable.HashMap[TopicIdPartition, Long]()

    fetchInfos.foreach { fetchInfo =>
      val (tp, fetchPartitionData) = fetchInfo
      val isDiskless = topicView.isDisklessTopic(tp.topic)
      var partitionLookupFailed = false
      if (!isDiskless) {
        classicFetchInfos += fetchInfo
      } else {
        val classicToDisklessStartOffset = topicView.getClassicToDisklessStartOffset(tp.topicPartition())
        // partitions with switching in progress should always serve from local log
        var shouldReadFromUnifiedLog = classicToDisklessStartOffset == PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING
        if (consolidationActive(tp.topic)) {
          partitionOrError(tp.topicPartition) match {
            case Right(partition) =>
              // Deref partition.log once: it is swapped on log recreation and dir change, so
              // separate derefs can mix offsets from two different logs. The offsets below are still
              // read one at a time, and the consolidation fetcher can append between them. Read the
              // high watermark before LEO so that skew can only fail the supplement gate below, never
              // pass it on a stale anchor.
              val localLog = partition.log
              val localHighWatermark = localLog.map(_.highWatermark).getOrElse(0L)
              val logEndOffset = localLog.map(_.logEndOffset).getOrElse(0L)
              val localLogStartOffset = localLog.map(_.localLogStartOffset).getOrElse(0L)
              // Where the local read stops. Consumers are bounded by the high watermark because an
              // AZ-local replica may have appended data which is not fetchable yet. Followers and
              // future replicas are bounded by LEO, since LOG_END isolation permits them to
              // replicate the uncommitted suffix. A read_committed consumer stops at the last stable
              // offset instead, which this does not model: diskless carries no transactional data and
              // the switch aborts undecided transactions, so no consolidating partition has an
              // LSO below its high watermark. Derive the bound from params.isolation if that changes.
              val localReadFrontier =
                if (params.isFromConsumer) localHighWatermark else logEndOffset
              // Two ranges must stay on the local read path even when they sit above the isolation
              // frontier, because diskless cannot answer them: [logStartOffset, localLogStartOffset)
              // lives only in the remote tier once ConsolidatedDisklessLogPruner has pruned the
              // diskless batches, and [0, classicToDisklessStartOffset) is the classic prefix the
              // control plane never held. Both switch sentinels are negative, so a partition that
              // never switched keeps the frontier at the isolation bound. Without a local log there
              // is nothing to read locally and diskless is authoritative for the whole range.
              val readableFrontier =
                if (localLog.isEmpty) 0L
                else Math.max(Math.max(localReadFrontier, localLogStartOffset), classicToDisklessStartOffset)
              // A follower's local logStartOffset stays frozen at the switch. DeleteRecords and
              // retention advance only the real leader's log start and the control-plane cross-tier
              // earliest, never a follower's. With managed replicas the metadata transformer routes
              // consumers to a hash-selected replica (usually a follower), which then serves the read
              // from that stale local log, so the consumer could read records below the authoritative
              // earliest. Reject those with OFFSET_OUT_OF_RANGE so the consumer resets via
              // ListOffsets(EARLIEST). This covers every consumer fetch, not just pre-KIP-392 ones: a
              // modern consumer is served from the follower too (allowReplica is true for it once the
              // transformer points it at that replica). The leader is skipped since its own local
              // logStartOffset already advanced. Best effort: the cache is only ever stale-low, so a
              // few deleted records may survive until it refreshes.
              val mayServeFromFollowerLocalLog =
                params.isFromConsumer && !partition.isLeader
              val crossTierEarliest =
                if (mayServeFromFollowerLocalLog) crossTierEarliestOffset(tp.topicPartition())
                else OptionalLong.empty()
              if (crossTierEarliest.isPresent && fetchPartitionData.fetchOffset < crossTierEarliest.getAsLong) {
                immediateFetchResponses += tp -> new FetchPartitionData(
                  Errors.OFFSET_OUT_OF_RANGE,
                  UnifiedLog.UNKNOWN_OFFSET,
                  UnifiedLog.UNKNOWN_OFFSET,
                  MemoryRecords.EMPTY,
                  Optional.empty(),
                  OptionalLong.empty(),
                  Optional.empty(),
                  OptionalInt.empty(),
                  false
                )
                partitionLookupFailed = true
              } else if (fetchPartitionData.fetchOffset < readableFrontier) {
                shouldReadFromUnifiedLog = true
                // Same population as isBelowSealAndAheadOfLocalLog: inside this branch only the seal
                // can have raised the frontier. Keep the two in step. Marked here, not in the
                // carve-out, since DelayedFetch re-reads on completion and would count twice.
                // Followers ask for their own LEO on a leader below the seal, so they are excluded.
                if (params.isFromConsumer && fetchPartitionData.fetchOffset >= logEndOffset)
                  disklessSwitchedPrefixMissingFetchRate.mark()
                // Only a consumer read that starts inside the local log and reaches its end can take
                // a supplement. Outside that window the anchor asks object storage for classic
                // offsets, or an empty read passes the exhaustion guard in
                // buildConsolidationSupplementFetchInfos. A pending switch has no committed seal.
                if (engineEnabled &&
                  params.isFromConsumer &&
                  classicToDisklessStartOffset != PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING &&
                  fetchPartitionData.fetchOffset >= localLogStartOffset &&
                  fetchPartitionData.fetchOffset < logEndOffset &&
                  localReadFrontier >= logEndOffset)
                  consolidatingLocalFetchSupplements += (tp -> logEndOffset)
              } else if (!shouldReadFromUnifiedLog && fetchPartitionData.fetchOffset < logEndOffset) {
                // The local log holds this offset but cannot serve it, so this read goes to Inkless.
                // A switch-pending partition is excluded because it was already routed to the local
                // log above, and a follower cannot reach this branch: its frontier is LEO.
                consolidationHighWatermarkLagFetchRate.mark()
              }
              // else: the fetch is at or beyond the frontier and beyond the local log, so diskless
              // is authoritative
            case Left(error) =>
              warn(s"Error while fetching partition ${tp.topicPartition()} for consolidating diskless topic: $error. " +
                s"Returning error for the fetch request since we cannot determine if the partition has switched to diskless or not.")
              immediateFetchResponses +=
                tp ->
                  new FetchPartitionData(
                    error,
                    UnifiedLog.UNKNOWN_OFFSET,
                    UnifiedLog.UNKNOWN_OFFSET,
                    MemoryRecords.EMPTY,
                    Optional.empty(),
                    OptionalLong.empty(),
                    Optional.empty(),
                    OptionalInt.empty(),
                    false
                  )
              partitionLookupFailed = true
          }
        } else {
          shouldReadFromUnifiedLog = shouldReadFromUnifiedLog ||
            (classicToDisklessStartOffset >= 0 && fetchPartitionData.fetchOffset < classicToDisklessStartOffset)
        }

        if (!partitionLookupFailed) {
          val disklessSwitchCompleted = !shouldReadFromUnifiedLog && classicToDisklessStartOffset >= 0
          if (params.isFromFollower && disklessSwitchCompleted) {
            var fetchError = Errors.NONE
            var divergingEpoch = Optional.empty[FetchResponseData.EpochEndOffset]
            // A recovered follower for a switched partition may already be caught up to the
            // seal offset but still be outside ISR. Record the seal-offset fetch so the normal
            // ISR expansion path can observe that the follower is caught up without reading
            // diskless data into the local log.
            if (fetchPartitionData.fetchOffset >= classicToDisklessStartOffset) {
              partitionOrError(tp.topicPartition) match {
                case Right(partition) =>
                  try {
                    // Use the classic follower-read validation without returning any records.
                    val fetchAtSeal = new PartitionData(
                      fetchPartitionData.topicId,
                      classicToDisklessStartOffset,
                      fetchPartitionData.logStartOffset,
                      0,
                      fetchPartitionData.currentLeaderEpoch,
                      fetchPartitionData.lastFetchedEpoch
                    )
                    val readInfo = partition.fetchRecords(
                      fetchParams = params,
                      fetchPartitionData = fetchAtSeal,
                      fetchTimeMs = time.milliseconds,
                      maxBytes = 0,
                      minOneMessage = false,
                      updateFetchState = true
                    )
                    divergingEpoch = readInfo.divergingEpoch
                  } catch {
                    case NonFatal(e) =>
                      fetchError = Errors.forException(e)
                      if (fetchError == Errors.UNKNOWN_SERVER_ERROR) {
                        error(s"Error validating at-seal fetch from " +
                          s"${FetchRequest.describeReplicaId(params.replicaId)} on partition $tp " +
                          s"at seal $classicToDisklessStartOffset", e)
                      }
                  }
                case Left(error) => fetchError = error
              }
            }
            // The partition has fully switched to diskless and the follower is asking for an offset at or beyond it.
            // Followers must never replicate diskless records into their local log.
            // Empty records and HW at the seal offset make the follower treat the local log as caught up.
            // ReplicaFetcherThread evicts once this replica is in ISR, or immediately if consolidating.
            // logStartOffset=0 is a no-op for the follower (maybeIncrementLogStartOffset only ever advances),
            // so classic local data stays in place and can still serve consumer reads.
            immediateFetchResponses += tp ->
              new FetchPartitionData(
                fetchError,
                if (fetchError == Errors.NONE) classicToDisklessStartOffset else UnifiedLog.UNKNOWN_OFFSET,
                if (fetchError == Errors.NONE) 0L else UnifiedLog.UNKNOWN_OFFSET,
                MemoryRecords.EMPTY,
                divergingEpoch,
                OptionalLong.empty(),
                Optional.empty(),
                OptionalInt.empty(),
                false
              )
          } else {
            (shouldReadFromUnifiedLog, managedReplicasEnabled) match {
              // Either born-diskless or completely switched to diskless
              case (false, _) =>
                maybeBackfillDisklessTopicId(tp) match {
                  case Some(backfilledTp) =>
                    disklessFetchInfos += (backfilledTp -> fetchPartitionData)
                    if (!backfilledTp.topicId.equals(tp.topicId)) disklessTopicIdOverrides += (backfilledTp -> tp)
                  case None =>
                    error(s"Got null topic id from KRaft metadata for diskless topic ${tp.topic}")
                    immediateFetchResponses += tp -> new FetchPartitionData(
                      Errors.UNKNOWN_TOPIC_ID,
                      UnifiedLog.UNKNOWN_OFFSET,
                      UnifiedLog.UNKNOWN_OFFSET,
                      MemoryRecords.EMPTY,
                      Optional.empty(),
                      OptionalLong.empty(),
                      Optional.empty(),
                      OptionalInt.empty(),
                      false
                    )
                }
              // Local log has data, managed replicas enabled — serve from local log
              case (true, true) =>
                classicFetchInfos += fetchInfo
              // Cannot read from UnifiedLog on a diskless topic if diskless managed replicas are not enabled.
              case (true, false) =>
                warn(s"Fetch from replica ${params.replicaId} for diskless topic " +
                  s"${tp.topic} partition ${tp.partition} with fetch offset ${fetchPartitionData.fetchOffset} rejected: " +
                  s"local log has data but managed replicas are not enabled.")
                immediateFetchResponses += tp -> new FetchPartitionData(
                  Errors.INVALID_REQUEST,
                  UnifiedLog.UNKNOWN_OFFSET,
                  UnifiedLog.UNKNOWN_OFFSET,
                  MemoryRecords.EMPTY,
                  Optional.empty(),
                  OptionalLong.empty(),
                  Optional.empty(),
                  OptionalInt.empty(),
                  false
                )
            }
          }
        }
      }
    }

    new DisklessFetchPlan(classicFetchInfos.toSeq, disklessFetchInfos.toSeq, immediateFetchResponses.toSeq,
      consolidatingLocalFetchSupplements.toMap, disklessTopicIdOverrides.toMap)
  }

  // Older fetch versions (<13) don't have topicId in the request -- backfill it for backward compatibility
  private def maybeBackfillDisklessTopicId(topicIdPartition: TopicIdPartition): Option[TopicIdPartition] = {
    if (topicIdPartition.topicId().equals(Uuid.ZERO_UUID)) {
      topicView.getTopicId(topicIdPartition.topic()) match {
        case Uuid.ZERO_UUID => None
        case topicId => Some(new TopicIdPartition(topicId, topicIdPartition.topicPartition()))
      }
    } else {
      Some(topicIdPartition)
    }
  }
}

/**
 * The routes of one fetch. Consolidating supplements map a partition served from its local tier to
 * the local log end offset, where a diskless read may continue it.
 */
final class DisklessFetchPlan(val classic: Seq[(TopicIdPartition, PartitionData)],
                              val diskless: Seq[(TopicIdPartition, PartitionData)],
                              val immediate: Seq[(TopicIdPartition, FetchPartitionData)],
                              val consolidatingSupplements: Map[TopicIdPartition, Long],
                              topicIdOverrides: Map[TopicIdPartition, TopicIdPartition]) {
  /**
   * Returns the response with the client's original partition identities, followed by the immediate
   * responses. Legacy fetches key partitions by `Uuid.ZERO_UUID`, and the fetch session looks them up
   * by that key, so a diskless read with a resolved topic ID must answer under the original key.
   */
  def respond(response: Seq[(TopicIdPartition, FetchPartitionData)]): Seq[(TopicIdPartition, FetchPartitionData)] = {
    val restored =
      if (topicIdOverrides.isEmpty) response
      else response.map { case (resolved, data) => topicIdOverrides.getOrElse(resolved, resolved) -> data }
    restored ++ immediate
  }
}
