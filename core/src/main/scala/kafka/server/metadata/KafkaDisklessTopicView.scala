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
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.{Node, TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.metadata.{KRaftMetadataCache, PartitionRegistration}

import java.util
import java.util.stream.{Collectors, IntStream}

/** Reads diskless topic state from the broker's KRaft metadata cache. */
class KafkaDisklessTopicView(val metadataCache: KRaftMetadataCache) extends DisklessTopicView {

  override def getAliveBrokerNodes(listenerName: ListenerName): util.List[Node] = {
    metadataCache.getAliveBrokerNodes(listenerName)
  }

  override def getTopicId(topicName: String): Uuid = {
    metadataCache.getTopicId(topicName)
  }

  override def isDisklessTopic(topicName: String): Boolean = {
    metadataCache.topicConfig(topicName).getProperty(TopicConfig.DISKLESS_ENABLE_CONFIG, "false").toBoolean
  }

  override def isRemoteStorageEnabled(topicName: String): Boolean = {
    metadataCache.topicConfig(topicName).getProperty(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false").toBoolean
  }

  override def isConsolidatingDisklessTopic(topicName: String): Boolean = {
    isDisklessTopic(topicName) && isRemoteStorageEnabled(topicName)
  }

  override def getDisklessTopicPartitions: util.Set[TopicIdPartition] = {
    metadataCache.getAllTopics().stream()
      .filter(isDisklessTopic)
      .flatMap(t => IntStream.range(0, metadataCache.numPartitions(t).get())
        .mapToObj(p => new TopicIdPartition(metadataCache.getTopicId(t), p, t)))
      .collect(Collectors.toSet[TopicIdPartition]())
  }

  override def getConsolidatingDisklessTopicPartitions: util.Set[TopicIdPartition] = {
    metadataCache.getAllTopics().stream()
      .filter(isConsolidatingDisklessTopic)
      .flatMap(t => IntStream.range(0, metadataCache.numPartitions(t).get())
        .mapToObj(p => new TopicIdPartition(metadataCache.getTopicId(t), p, t)))
      .collect(Collectors.toSet[TopicIdPartition]())
  }

  override def getClassicToDisklessStartOffset(topicPartition: TopicPartition): Long = {
    Option(metadataCache.currentImage().topics().getTopic(topicPartition.topic()))
      .flatMap(topicImage => Option(topicImage.partitions().get(topicPartition.partition())))
      .map(_.classicToDisklessStartOffset)
      .getOrElse(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET)
  }

  override def isReplicaInIsr(topicPartition: TopicPartition, replicaId: Int): Boolean = {
    Option(metadataCache.currentImage().topics().getTopic(topicPartition.topic()))
      .flatMap(topicImage => Option(topicImage.partitions().get(topicPartition.partition())))
      .exists(_.isr.contains(replicaId))
  }

  /**
   * The diskless leader epoch (E_d) captured at the classic-to-diskless switch, or
   * [[PartitionRegistration.NO_DISKLESS_LEADER_EPOCH]] when the partition never switched (born-diskless
   * or switch still pending). It is strictly greater than every classic-prefix leader epoch.
   */
  override def getDisklessLeaderEpoch(topicPartition: TopicPartition): Int = {
    Option(metadataCache.currentImage().topics().getTopic(topicPartition.topic()))
      .flatMap(topicImage => Option(topicImage.partitions().get(topicPartition.partition())))
      .map(_.disklessLeaderEpoch)
      .getOrElse(PartitionRegistration.NO_DISKLESS_LEADER_EPOCH)
  }
}
