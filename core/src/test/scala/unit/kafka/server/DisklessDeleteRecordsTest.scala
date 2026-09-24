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

import io.aiven.inkless.engine.RecordDeletion
import kafka.cluster.Partition
import kafka.server.metadata.DisklessTopicView
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsPartitionResult
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.metadata.PartitionRegistration
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._

class DisklessDeleteRecordsTest {
  private val view = mock(classOf[DisklessTopicView])
  when(view.isDisklessTopic(any())).thenAnswer(invocation => invocation.getArgument[String](0) != "classic")
  when(view.getClassicToDisklessStartOffset(any())).thenAnswer { invocation =>
    invocation.getArgument[TopicPartition](0).topic match {
      case "switched" => 100L
      case _ => PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET
    }
  }
  private val classic = new TopicPartition("classic", 0)
  private val diskless = new TopicPartition("diskless", 0)
  private val switched = new TopicPartition("switched", 0)

  private def deleter(withDeletion: Boolean): DisklessDeleteRecords =
    new DisklessDeleteRecords(view,
      if (withDeletion) Some(mock(classOf[RecordDeletion])) else None, None,
      (_: TopicPartition) => Left[Errors, Partition](Errors.NOT_LEADER_OR_FOLLOWER), false)

  @Test
  def testSplitsSwitchedPartitionsAtTheSeal(): Unit = {
    val plan = deleter(withDeletion = true).split(Map(classic -> 5L, diskless -> 7L, switched -> 150L))

    assertEquals(Map(classic -> 5L, switched -> 100L), plan.local)
    assertEquals(Map(diskless -> 7L, switched -> 150L), plan.diskless)
    assertTrue(plan.failed.isEmpty)

    val localFailure = new DeleteRecordsPartitionResult().setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code)
    assertEquals(Map(diskless -> 7L), plan.disklessAfterLocal(Map(switched -> localFailure)))
  }

  @Test
  def testFailsDisklessPartitionsWhenTheEngineCannotDelete(): Unit = {
    val plan = deleter(withDeletion = false).split(Map(classic -> 5L, diskless -> 7L))

    assertEquals(Map(classic -> 5L), plan.local)
    assertTrue(plan.diskless.isEmpty)
    assertEquals(Set(diskless), plan.failed.keySet)
    assertEquals(Errors.UNKNOWN_SERVER_ERROR.code, plan.failed(diskless).errorCode)
  }
}
