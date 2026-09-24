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
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.internal.MemoryRecords
import org.apache.kafka.common.requests.FetchRequest.PartitionData
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.metadata.PartitionRegistration
import org.apache.kafka.server.storage.log.{FetchIsolation, FetchParams, FetchPartitionData}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._

import java.util.{Optional, OptionalInt, OptionalLong}

class DisklessFetchPlannerTest {
  private val topicId = Uuid.randomUuid()
  private val classic = new TopicIdPartition(Uuid.randomUuid(), 0, "classic")
  private val diskless = new TopicIdPartition(topicId, 0, "diskless")
  private val switched = new TopicIdPartition(Uuid.randomUuid(), 0, "switched")
  private val pending = new TopicIdPartition(Uuid.randomUuid(), 0, "pending")

  private val view = mock(classOf[DisklessTopicView])
  when(view.isDisklessTopic(any())).thenAnswer(invocation => invocation.getArgument[String](0) != "classic")
  when(view.getClassicToDisklessStartOffset(any())).thenAnswer { invocation =>
    invocation.getArgument[TopicPartition](0).topic match {
      case "switched" => 100L
      case "pending" => PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING
      case _ => PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET
    }
  }
  when(view.getTopicId("diskless")).thenReturn(topicId)

  private def planner(managedReplicas: Boolean): DisklessFetchPlanner =
    new DisklessFetchPlanner(view, (_: TopicPartition) => Left[Errors, Partition](Errors.NOT_LEADER_OR_FOLLOWER),
      (_: TopicPartition) => OptionalLong.empty(), (_: String) => false, true, managedReplicas, new MockTime(),
      mock(classOf[Meter]), mock(classOf[Meter]))

  private def consumerParams: FetchParams =
    new FetchParams(-1, -1L, 0, 0, 1024, FetchIsolation.HIGH_WATERMARK, Optional.empty(), false)

  private def fetchAt(partition: TopicIdPartition, offset: Long): (TopicIdPartition, PartitionData) =
    partition -> new PartitionData(partition.topicId, offset, 0L, 1024, Optional.empty())

  @Test
  def testRoutesByTopicTypeAndSwitchState(): Unit = {
    val plan = planner(managedReplicas = true).plan(consumerParams, Seq(
      fetchAt(classic, 0L), fetchAt(diskless, 0L), fetchAt(switched, 10L), fetchAt(switched, 100L), fetchAt(pending, 0L)))

    assertEquals(Seq(classic, switched, pending), plan.classic.map(_._1))
    assertEquals(Seq(diskless, switched), plan.diskless.map(_._1))
    assertEquals(Seq(100L), plan.diskless.filter(_._1 == switched).map(_._2.fetchOffset))
    assertTrue(plan.immediate.isEmpty)
    assertTrue(plan.consolidatingSupplements.isEmpty)
  }

  @Test
  def testRejectsLocalReadsOfDisklessTopicsWithoutManagedReplicas(): Unit = {
    val plan = planner(managedReplicas = false).plan(consumerParams, Seq(fetchAt(switched, 10L)))

    assertTrue(plan.classic.isEmpty)
    assertEquals(Seq(switched -> Errors.INVALID_REQUEST), plan.immediate.map { case (tp, data) => tp -> data.error })
  }

  @Test
  def testAnswersLegacyFetchesUnderTheirOriginalPartitionKey(): Unit = {
    val legacy = new TopicIdPartition(Uuid.ZERO_UUID, diskless.topicPartition)
    val plan = planner(managedReplicas = true).plan(consumerParams, Seq(fetchAt(legacy, 0L)))
    assertEquals(Seq(diskless), plan.diskless.map(_._1))

    val data = new FetchPartitionData(Errors.NONE, 1L, 0L, MemoryRecords.EMPTY, Optional.empty(),
      OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false)
    assertEquals(Seq(legacy), plan.respond(Seq(diskless -> data)).map(_._1))
  }
}
