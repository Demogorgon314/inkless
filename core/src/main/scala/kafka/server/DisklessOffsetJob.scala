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

import io.aiven.inkless.engine.DisklessEngine
import io.aiven.inkless.engine.DisklessEngine.{ListOffsetsResult, ListOffsetsSpec}
import kafka.server.metadata.InklessMetadataView
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.internal.FileRecords
import org.apache.kafka.storage.internals.log.OffsetResultHolder.FileRecordsOrError

import java.util.{LinkedHashMap, Optional}
import java.util.concurrent.{CompletableFuture, Future}
import scala.jdk.CollectionConverters._

/**
 * Collects one broker ListOffsets batch and adapts engine results to Kafka's purgatory.
 * Topic identities are fixed when added. Each asynchronous hybrid fallback uses a fresh job.
 */
class DisklessOffsetJob(engine: DisklessEngine, metadata: InklessMetadataView) {
  private val requests = new LinkedHashMap[TopicIdPartition, ListOffsetsSpec]()
  private val results = new LinkedHashMap[TopicIdPartition, CompletableFuture[FileRecordsOrError]]()
  private val cancellation = new CompletableFuture[Void]()
  private var started = false

  def add(partition: TopicPartition, request: ListOffsetsPartition): CompletableFuture[FileRecordsOrError] = {
    require(!started, "Cannot add partitions after starting an offset job")
    val topicId = metadata.getTopicId(partition.topic())
    if (topicId == null || topicId == Uuid.ZERO_UUID)
      return CompletableFuture.completedFuture(error(Errors.UNKNOWN_TOPIC_OR_PARTITION.exception()))
    val id = new TopicIdPartition(topicId, partition)
    require(!requests.containsKey(id), "Duplicate partition in offset job")
    val epoch = if (request.currentLeaderEpoch() < 0) Optional.empty[Integer]()
      else Optional.of[Integer](request.currentLeaderEpoch())
    requests.put(id, new ListOffsetsSpec(request.timestamp(), epoch))
    val result = new CompletableFuture[FileRecordsOrError]()
    results.put(id, result)
    result
  }

  def mustHandle(topic: String): Boolean = metadata.isDisklessTopic(topic)

  def cancelHandler(): Future[Void] = cancellation

  def start(): Unit = {
    require(!started, "Offset job already started")
    started = true
    cancellation.whenComplete { (_, _) =>
      if (cancellation.isCancelled) results.values().asScala.foreach(_.cancel(false))
    }
    if (requests.isEmpty || cancellation.isCancelled) return
    try {
      val pending = engine.listOffsets(requests)
      cancellation.whenComplete { (_, _) =>
        if (cancellation.isCancelled) pending.cancel(true)
      }
      pending.whenComplete { (response, failure) =>
        results.asScala.foreach { case (id, result) =>
          if (cancellation.isCancelled) result.cancel(false)
          else if (failure != null) result.complete(error(Errors.forException(failure).exception()))
          else result.complete(translate(Option(response).flatMap(r => Option(r.get(id)))))
        }
      }
    } catch {
      case failure: Exception => results.values().asScala.foreach(_.complete(error(failure)))
    }
  }

  private def translate(result: Option[ListOffsetsResult]): FileRecordsOrError = result match {
    case None => error(Errors.UNKNOWN_SERVER_ERROR.exception())
    case Some(offset) if offset.error() != Errors.NONE => error(offset.error().exception())
    case Some(offset) =>
      val value = if (offset.offset() < 0) Optional.empty[FileRecords.TimestampAndOffset]()
        else Optional.of(new FileRecords.TimestampAndOffset(offset.timestamp(), offset.offset(), offset.leaderEpoch()))
      new FileRecordsOrError(Optional.empty(), value)
  }

  private def error(failure: Exception): FileRecordsOrError =
    new FileRecordsOrError(Optional.of(failure), Optional.empty())
}
