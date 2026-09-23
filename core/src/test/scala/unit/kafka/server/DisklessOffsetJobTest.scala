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

import io.aiven.inkless.engine.OffsetReader
import io.aiven.inkless.engine.OffsetReader.{ListOffsetsResult, ListOffsetsSpec}
import kafka.server.metadata.InklessMetadataView
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition
import org.apache.kafka.common.protocol.Errors
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._

import java.util.{Map => JMap, Optional}
import java.util.concurrent.CompletableFuture

class DisklessOffsetJobTest {
  private val engine = mock(classOf[OffsetReader])
  private val metadata = mock(classOf[InklessMetadataView])
  private val partition = new TopicPartition("topic", 0)
  private val id = new TopicIdPartition(Uuid.randomUuid(), partition)
  private val request = new ListOffsetsPartition().setPartitionIndex(0).setTimestamp(-1L)
  when(metadata.getTopicId("topic")).thenReturn(id.topicId())

  @Test
  def preservesTopicIdentityWhenNameIsRecreatedBeforeDispatch(): Unit = {
    val job = new DisklessOffsetJob(engine, metadata)
    val result = job.add(partition, request)
    when(metadata.getTopicId("topic")).thenReturn(Uuid.randomUuid())
    when(engine.listOffsets(any())).thenAnswer { invocation =>
      val batch = invocation.getArgument[JMap[TopicIdPartition, ListOffsetsSpec]](0)
      assertEquals(java.util.Set.of(id), batch.keySet())
      CompletableFuture.completedFuture(JMap.of(id,
        new ListOffsetsResult(Errors.NONE, 10L, 12L, Optional.of[Integer](3))))
    }
    job.start()
    assertEquals(12L, result.join().timestampAndOffset().get().offset)
    assertEquals(Optional.of[Integer](3), result.join().timestampAndOffset().get().leaderEpoch)
  }

  @Test
  def cancellationBeforeDispatchDoesNotCallEngine(): Unit = {
    val job = new DisklessOffsetJob(engine, metadata)
    val result = job.add(partition, request)
    job.cancelHandler().cancel(false)
    job.start()
    assertTrue(result.isCancelled)
    verifyNoInteractions(engine)
  }

  @Test
  def cancellationAfterDispatchCancelsWaitingAndBackendFuture(): Unit = {
    val pending = new CompletableFuture[JMap[TopicIdPartition, ListOffsetsResult]]()
    when(engine.listOffsets(any())).thenReturn(pending)
    val job = new DisklessOffsetJob(engine, metadata)
    val result = job.add(partition, request)
    job.start()
    job.cancelHandler().cancel(false)
    assertTrue(result.isCancelled)
    assertTrue(pending.isCancelled)
  }

  @Test
  def missingPartitionResponseCompletesWithErrorInsteadOfHanging(): Unit = {
    when(engine.listOffsets(any())).thenReturn(CompletableFuture.completedFuture(JMap.of()))
    val job = new DisklessOffsetJob(engine, metadata)
    val result = job.add(partition, request)
    job.start()
    assertEquals(Errors.UNKNOWN_SERVER_ERROR, Errors.forException(result.join().exception().get()))
  }

  @Test
  def missingTopicDoesNotReachStorage(): Unit = {
    when(metadata.getTopicId("topic")).thenReturn(Uuid.ZERO_UUID)
    val job = new DisklessOffsetJob(engine, metadata)
    val result = job.add(partition, request)
    job.start()
    assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.forException(result.join().exception().get()))
    verifyNoInteractions(engine)
  }

  @Test
  def synchronousStorageFailureCompletesAllPartitions(): Unit = {
    when(engine.listOffsets(any())).thenThrow(Errors.KAFKA_STORAGE_ERROR.exception())
    val job = new DisklessOffsetJob(engine, metadata)
    val first = job.add(partition, request)
    val second = job.add(new TopicPartition("topic", 1), request)
    job.start()
    assertEquals(Errors.KAFKA_STORAGE_ERROR, Errors.forException(first.join().exception().get()))
    assertEquals(Errors.KAFKA_STORAGE_ERROR, Errors.forException(second.join().exception().get()))
  }
}
