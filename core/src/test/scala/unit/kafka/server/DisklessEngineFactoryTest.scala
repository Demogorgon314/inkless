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

import io.aiven.inkless.engine.{DisklessEngine, DisklessStorageProvider, LogTiering, LogTransition}
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.metadata.KRaftMetadataCache
import org.apache.kafka.server.util.Scheduler
import org.apache.kafka.storage.log.metrics.BrokerTopicStats
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._

import java.util.Optional

class DisklessEngineFactoryTest {
  private def config(allowFromClassic: Boolean, consolidation: Boolean): KafkaConfig = {
    val config = mock(classOf[KafkaConfig])
    when(config.disklessAllowFromClassicEnabled).thenReturn(allowFromClassic)
    when(config.disklessRemoteStorageConsolidationEnabled).thenReturn(consolidation)
    config
  }

  private def engine(transition: Boolean, tiering: Boolean): DisklessEngine = {
    val engine = mock(classOf[DisklessEngine])
    when(engine.logTransition()).thenReturn(
      if (transition) Optional.of(mock(classOf[LogTransition])) else Optional.empty[LogTransition]())
    when(engine.logTiering()).thenReturn(
      if (tiering) Optional.of(mock(classOf[LogTiering])) else Optional.empty[LogTiering]())
    engine
  }

  @Test
  def testAcceptsEnginesWithoutExtensionsWhenNoDependentFeatureIsEnabled(): Unit = {
    DisklessEngineFactory.validateFeatures(config(allowFromClassic = false, consolidation = false),
      engine(transition = false, tiering = false))
  }

  @Test
  def testNamesEveryEnabledFeatureWhoseExtensionIsMissing(): Unit = {
    val error = assertThrows(classOf[ConfigException], () => DisklessEngineFactory.validateFeatures(
      config(allowFromClassic = true, consolidation = true), engine(transition = false, tiering = false)))
    assertTrue(error.getMessage.contains("LogTransition"), error.getMessage)
    assertTrue(error.getMessage.contains("LogTiering"), error.getMessage)

    DisklessEngineFactory.validateFeatures(config(allowFromClassic = true, consolidation = true),
      engine(transition = true, tiering = true))
  }

  @Test
  def testClosesTheEngineWhenItLacksAnEnabledFeature(): Unit = {
    val unsupported = engine(transition = false, tiering = true)
    val provider = mock(classOf[DisklessStorageProvider])
    when(provider.createBrokerEngine(any())).thenReturn(unsupported)
    val brokerConfig = config(allowFromClassic = true, consolidation = false)

    assertThrows(classOf[ConfigException], () => DisklessEngineFactory.create(brokerConfig,
      mock(classOf[Scheduler]), mock(classOf[KRaftMetadataCache]), new BrokerTopicStats(), Some(provider)))
    verify(unsupported).close()
  }
}
