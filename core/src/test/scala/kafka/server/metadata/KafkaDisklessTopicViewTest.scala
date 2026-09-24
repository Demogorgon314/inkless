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
package kafka.server.metadata

import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.{DirectoryId, TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.image.{MetadataImage, TopicImage, TopicsImage}
import org.apache.kafka.metadata.{KRaftMetadataCache, LeaderRecoveryState, PartitionRegistration}
import org.junit.jupiter.api.{BeforeEach, Test}
import org.junit.jupiter.api.Assertions._
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito._

import java.util
import java.util.{Collections, Optional, Properties}

class KafkaDisklessTopicViewTest {
  private var metadataCache: KRaftMetadataCache = _
  private var metadataView: KafkaDisklessTopicView = _

  @BeforeEach
  def setup(): Unit = {
    metadataCache = mock(classOf[KRaftMetadataCache])
    metadataView = new KafkaDisklessTopicView(metadataCache)
  }

  @Test
  def testIsDisklessTopic(): Unit = {
    val metadataCache = mock(classOf[KRaftMetadataCache])
    val metadataView = new KafkaDisklessTopicView(metadataCache)

    // Each tuple contains: (description, properties object, expected result)
    val testCases = Seq(
      ("diskless=true", createTopicProps(diskless = Some("true")), true),
      ("case-insensitive", createTopicProps(diskless = Some("TRUE")), true),
      ("empty properties", new Properties(), false),
      ("unrelated properties", {val p = new Properties(); p.put("foo", "bar"); p}, false),
    )

    testCases.foreach { case (description, props, expected) =>
      val topicName = description.replaceAll(" ", "-")
      when(metadataCache.topicConfig(topicName)).thenReturn(props)
      assertEquals(expected, metadataView.isDisklessTopic(topicName), s"Failed on case: '$description'")
    }
  }

  @Test
  def testIsRemoteStorageEnabled(): Unit = {
    val metadataCache = mock(classOf[KRaftMetadataCache])
    val metadataView = new KafkaDisklessTopicView(metadataCache)

    // Each tuple contains: (description, properties object, expected result)
    val testCases = Seq(
      ("remote.log.storage.enable=true", createTopicProps(remoteStorageEnable = Some("true")), true),
      ("case-insensitive", createTopicProps(remoteStorageEnable = Some("TRUE")), true),
      ("remote.log.storage.enable=false", createTopicProps(remoteStorageEnable = Some("false")), false),
      ("empty properties", new Properties(), false),
      ("unrelated properties", {val p = new Properties(); p.put("foo", "bar"); p}, false),
    )

    testCases.foreach { case (description, props, expected) =>
      val topicName = description.replaceAll(" ", "-")
      when(metadataCache.topicConfig(topicName)).thenReturn(props)
      assertEquals(expected, metadataView.isRemoteStorageEnabled(topicName), s"Failed on case: '$description'")
    }
  }

  @Test
  def testIsConsolidatingDisklessTopicTrueOnlyWhenDisklessAndRemoteStorageEnabled(): Unit = {
    val metadataCache = mock(classOf[KRaftMetadataCache])
    val metadataView = new KafkaDisklessTopicView(metadataCache)

    val consolidating = createTopicProps(diskless = Some("true"), remoteStorageEnable = Some("true"))
    when(metadataCache.topicConfig("consolidating")).thenReturn(consolidating)
    assertTrue(metadataView.isConsolidatingDisklessTopic("consolidating"))

    when(metadataCache.topicConfig("diskless-only")).thenReturn(createTopicProps(diskless = Some("true")))
    assertFalse(metadataView.isConsolidatingDisklessTopic("diskless-only"))

    when(metadataCache.topicConfig("remote-only")).thenReturn(createTopicProps(remoteStorageEnable = Some("true")))
    assertFalse(metadataView.isConsolidatingDisklessTopic("remote-only"))

    when(metadataCache.topicConfig("neither")).thenReturn(new Properties())
    assertFalse(metadataView.isConsolidatingDisklessTopic("neither"))
  }

  @Test
  def testGetConsolidatingDisklessTopicPartitionsEmptyWhenNoTopics(): Unit = {
    val metadataCache = mock(classOf[KRaftMetadataCache])
    val metadataView = new KafkaDisklessTopicView(metadataCache)
    when(metadataCache.getAllTopics()).thenReturn(Collections.emptySet())
    assertTrue(metadataView.getConsolidatingDisklessTopicPartitions.isEmpty)
  }

  @Test
  def testGetConsolidatingDisklessTopicPartitionsExcludesTopicsThatAreNotConsolidating(): Unit = {
    val metadataCache = mock(classOf[KRaftMetadataCache])
    val metadataView = new KafkaDisklessTopicView(metadataCache)
    val topics = new util.HashSet[String]()
    topics.add("plain")
    topics.add("diskless-only")
    when(metadataCache.getAllTopics()).thenReturn(topics)
    when(metadataCache.topicConfig("plain")).thenReturn(new Properties())
    when(metadataCache.topicConfig("diskless-only")).thenReturn(createTopicProps(diskless = Some("true")))

    assertTrue(metadataView.getConsolidatingDisklessTopicPartitions.isEmpty)
    verify(metadataCache, never()).numPartitions(anyString())
  }

  @Test
  def testGetConsolidatingDisklessTopicPartitionsReturnsTopicIdPartitionPerPartition(): Unit = {
    val metadataCache = mock(classOf[KRaftMetadataCache])
    val metadataView = new KafkaDisklessTopicView(metadataCache)
    val uuidA = Uuid.randomUuid()
    val uuidB = Uuid.randomUuid()
    val topics = new util.HashSet[String]()
    topics.add("topic-a")
    topics.add("topic-b")
    when(metadataCache.getAllTopics()).thenReturn(topics)
    val consolidatingProps = createTopicProps(diskless = Some("true"), remoteStorageEnable = Some("true"))
    when(metadataCache.topicConfig("topic-a")).thenReturn(consolidatingProps)
    when(metadataCache.topicConfig("topic-b")).thenReturn(consolidatingProps)
    when(metadataCache.numPartitions("topic-a")).thenReturn(Optional.of(2))
    when(metadataCache.numPartitions("topic-b")).thenReturn(Optional.of(1))
    when(metadataCache.getTopicId("topic-a")).thenReturn(uuidA)
    when(metadataCache.getTopicId("topic-b")).thenReturn(uuidB)

    val result = metadataView.getConsolidatingDisklessTopicPartitions

    assertEquals(3, result.size)
    assertTrue(result.contains(new TopicIdPartition(uuidA, 0, "topic-a")))
    assertTrue(result.contains(new TopicIdPartition(uuidA, 1, "topic-a")))
    assertTrue(result.contains(new TopicIdPartition(uuidB, 0, "topic-b")))
  }

  private def createTopicProps(diskless: Option[String] = None, remoteStorageEnable: Option[String] = None): Properties = {
    val props = new Properties()
    diskless.foreach(v => props.put(TopicConfig.DISKLESS_ENABLE_CONFIG, v))
    remoteStorageEnable.foreach(v => props.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, v))
    props
  }

  private def partitionRegistration(disklessLeaderEpoch: Option[Int] = None, seal: Option[Long] = None): PartitionRegistration = {
    val builder = new PartitionRegistration.Builder()
      .setReplicas(Array(1))
      .setDirectories(DirectoryId.unassignedArray(1))
      .setIsr(Array(1))
      .setLeader(1)
      .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
      .setLeaderEpoch(3)
      .setPartitionEpoch(0)
    disklessLeaderEpoch.foreach(builder.setDisklessLeaderEpoch)
    seal.foreach(builder.setClassicToDisklessStartOffset)
    builder.build()
  }

  // Wires metadataCache.currentImage().topics().getTopic(topic) -> topicImage holding the given partitions.
  private def stubImageTopic(topic: String, partitions: util.Map[Integer, PartitionRegistration]): Unit = {
    val image = mock(classOf[MetadataImage])
    val topicsImage = mock(classOf[TopicsImage])
    val topicImage = mock(classOf[TopicImage])
    when(metadataCache.currentImage()).thenReturn(image)
    when(image.topics()).thenReturn(topicsImage)
    when(topicsImage.getTopic(topic)).thenReturn(topicImage)
    when(topicImage.partitions()).thenReturn(partitions)
  }

  // Wires metadataCache.currentImage().topics().getTopic(topic) -> null (topic absent from the image).
  private def stubImageWithoutTopic(topic: String): Unit = {
    val image = mock(classOf[MetadataImage])
    val topicsImage = mock(classOf[TopicsImage])
    when(metadataCache.currentImage()).thenReturn(image)
    when(image.topics()).thenReturn(topicsImage)
    when(topicsImage.getTopic(topic)).thenReturn(null.asInstanceOf[TopicImage])
  }

  @Test
  def testGetDisklessLeaderEpochReturnsValueFromImage(): Unit = {
    val tp = new TopicPartition("switched", 0)
    stubImageTopic(tp.topic(), util.Map.of(Integer.valueOf(0), partitionRegistration(disklessLeaderEpoch = Some(7))))
    assertEquals(7, metadataView.getDisklessLeaderEpoch(tp))
  }

  @Test
  def testGetDisklessLeaderEpochReturnsSentinelWhenUnset(): Unit = {
    // Born-diskless / never-switched partition carries no diskless epoch.
    val tp = new TopicPartition("born-diskless", 0)
    stubImageTopic(tp.topic(), util.Map.of(Integer.valueOf(0), partitionRegistration()))
    assertEquals(PartitionRegistration.NO_DISKLESS_LEADER_EPOCH, metadataView.getDisklessLeaderEpoch(tp))
  }

  @Test
  def testGetDisklessLeaderEpochReturnsSentinelWhenTopicMissing(): Unit = {
    val tp = new TopicPartition("missing", 0)
    stubImageWithoutTopic(tp.topic())
    assertEquals(PartitionRegistration.NO_DISKLESS_LEADER_EPOCH, metadataView.getDisklessLeaderEpoch(tp))
  }

  @Test
  def testGetDisklessLeaderEpochReturnsSentinelWhenPartitionMissing(): Unit = {
    val tp = new TopicPartition("switched", 1)
    // Topic present, but only partition 0 exists in the image.
    stubImageTopic(tp.topic(), util.Map.of(Integer.valueOf(0), partitionRegistration(disklessLeaderEpoch = Some(7))))
    assertEquals(PartitionRegistration.NO_DISKLESS_LEADER_EPOCH, metadataView.getDisklessLeaderEpoch(tp))
  }

  @Test
  def testGetClassicToDisklessStartOffsetReturnsValueFromImage(): Unit = {
    val tp = new TopicPartition("switched", 0)
    stubImageTopic(tp.topic(), util.Map.of(Integer.valueOf(0), partitionRegistration(seal = Some(42L))))
    assertEquals(42L, metadataView.getClassicToDisklessStartOffset(tp))
  }

  @Test
  def testGetClassicToDisklessStartOffsetReturnsSentinelWhenTopicMissing(): Unit = {
    val tp = new TopicPartition("missing", 0)
    stubImageWithoutTopic(tp.topic())
    assertEquals(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET, metadataView.getClassicToDisklessStartOffset(tp))
  }

  @Test
  def testIsReplicaInIsrUsesImageState(): Unit = {
    val tp = new TopicPartition("switched", 0)
    stubImageTopic(tp.topic(), util.Map.of(Integer.valueOf(0), partitionRegistration()))
    assertTrue(metadataView.isReplicaInIsr(tp, 1))
    assertFalse(metadataView.isReplicaInIsr(tp, 2))
  }

  @Test
  def testIsReplicaInIsrReturnsFalseWhenTopicMissing(): Unit = {
    val tp = new TopicPartition("missing", 0)
    stubImageWithoutTopic(tp.topic())
    assertFalse(metadataView.isReplicaInIsr(tp, 1))
  }
}
