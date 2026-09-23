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

import io.aiven.inkless.common.SharedState
import io.aiven.inkless.control_plane.ControlPlane
import io.aiven.inkless.engine.{DisklessEngine, DisklessEngines, InklessDisklessEngine}
import kafka.server.metadata.InklessMetadataView
import org.apache.kafka.common.utils.Time
import org.apache.kafka.metadata.KRaftMetadataCache
import org.apache.kafka.storage.internals.log.LogConfig
import org.apache.kafka.storage.log.metrics.BrokerTopicStats

import java.util.OptionalInt
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._

/** Assembles provider-specific resources once; request handlers only receive the engine. */
object DisklessEngineFactory {
  def create(config: KafkaConfig,
             time: Time,
             metadataCache: KRaftMetadataCache,
             metadata: InklessMetadataView,
             metrics: BrokerTopicStats,
             defaultLogConfig: () => LogConfig,
             controlPlane: Option[ControlPlane]): Option[DisklessEngine] = {
    if (config.originals.containsKey(DisklessEngines.CLASS_NAME_CONFIG)) {
      val context = new DisklessEngine.Context(time, config.brokerId, metrics, config.extractLogConfigMap,
        topic => metadata.getTopicId(topic),
        topic => metadata.getTopicConfig(topic).originals.asScala.map { case (k, v) => k -> v.toString }.asJava,
        topic => metadataCache.numPartitions(topic).map(n => OptionalInt.of(n)).orElse(OptionalInt.empty()),
        () => metadataCache.currentImage().highestOffsetAndEpoch().offset())
      Some(DisklessEngines.load(config.originals, () => throw new IllegalStateException("Missing engine class"), context))
    } else {
      controlPlane.map { cp =>
        val state = SharedState.initialize(time, config.brokerId, config.inklessConfig, metadata, cp,
          metrics, () => defaultLogConfig())
        try nativeEngine(config, state)
        catch {
          case failure: Throwable =>
            try state.close()
            catch { case closeFailure: Throwable => failure.addSuppressed(closeFailure) }
            throw failure
        }
      }
    }
  }

  def nativeEngine(config: KafkaConfig, state: SharedState): InklessDisklessEngine = {
    val consolidation = if (config.disklessRemoteStorageConsolidationEnabled) {
      Some(new InklessDisklessEngine.ConsolidationConfig(
        config.disklessConsolidationFetchMetadataThreadPoolSize,
        config.disklessConsolidationFetchDataThreadPoolSize,
        config.disklessConsolidationFetchLaggingRequestRateLimit,
        config.disklessConsolidationFindBatchesMaxPerPartition))
    } else None
    new InklessDisklessEngine(state, consolidation.toJava)
  }
}
