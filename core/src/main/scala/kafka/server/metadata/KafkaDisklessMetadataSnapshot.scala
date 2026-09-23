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

import io.aiven.inkless.engine.DisklessMetadataSnapshot
import io.aiven.inkless.engine.DisklessMetadataSnapshot.TopicMetadata
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.config.{ConfigResource, TopicConfig}
import org.apache.kafka.image.MetadataImage

import java.lang.{Boolean => JBoolean}
import java.util.Optional
import scala.jdk.CollectionConverters._

/** Keeps topic configuration and its revision tied to the image captured by the broker. */
final class KafkaDisklessMetadataSnapshot(image: MetadataImage) extends DisklessMetadataSnapshot {
  override def topic(topicId: Uuid): Optional[TopicMetadata] = {
    val topic = image.topics().getTopic(topicId)
    if (topic == null) return Optional.empty()
    val properties = image.configs().configProperties(new ConfigResource(ConfigResource.Type.TOPIC, topic.name()))
    if (!JBoolean.parseBoolean(properties.getProperty(TopicConfig.DISKLESS_ENABLE_CONFIG)))
      return Optional.empty()
    val configs = properties.stringPropertyNames().asScala.map(key => key -> properties.getProperty(key)).toMap.asJava
    Optional.of(new TopicMetadata(topic.id(), topic.name(), topic.partitions().size(), configs,
      image.highestOffsetAndEpoch().offset()))
  }
}
