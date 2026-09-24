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

import io.aiven.inkless.control_plane.ControlPlane
import io.aiven.inkless.engine.{DisklessEngine, DisklessEngineContext, DisklessLifecycleContext, DisklessStorageProvider, DisklessTopicLifecycle}
import io.aiven.inkless.engine.builtin.InklessStorageProvider
import io.aiven.inkless.engine.loader.DisklessEngines
import kafka.server.metadata.{InklessMetadataView, KafkaDisklessMetadataSnapshot}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.metadata.KRaftMetadataCache
import org.apache.kafka.server.util.Scheduler
import org.apache.kafka.storage.internals.log.LogConfig
import org.apache.kafka.storage.log.metrics.BrokerTopicStats

import java.util.Optional

/** Selects one storage provider and creates its broker and controller components. */
object DisklessEngineFactory {
  final class ControllerStorage(val lifecycle: DisklessTopicLifecycle) extends AutoCloseable {
    private val contracts = lifecycle match {
      case _: DisklessTopicLifecycle.RequestDriven if lifecycle.isInstanceOf[DisklessTopicLifecycle.MetadataDriven] =>
        throw new IllegalArgumentException("Lifecycle must implement exactly one execution contract")
      case service: DisklessTopicLifecycle.RequestDriven => (Some(service), None)
      case service: DisklessTopicLifecycle.MetadataDriven => (None, Some(service))
      case _ => throw new IllegalArgumentException("Lifecycle must implement an execution contract")
    }
    val requestLifecycle: Option[DisklessTopicLifecycle.RequestDriven] = contracts._1
    val metadataLifecycle: Option[DisklessTopicLifecycle.MetadataDriven] = contracts._2

    override def close(): Unit = lifecycle.close()
  }

  def createControllerStorage(config: KafkaConfig, controlPlane: Option[ControlPlane]): Option[ControllerStorage] = {
    if (!config.disklessStorageSystemEnabled) return None
    val provider: Option[DisklessStorageProvider] =
      if (DisklessEngines.isConfigured(config.originals)) Some(DisklessEngines.load(config.originals))
      else controlPlane.map(InklessStorageProvider.forController)
    provider.map { p =>
      val lifecycle = p.createTopicLifecycle(new DisklessLifecycleContext(DisklessEngines.providerConfigs(config.originals)))
      try new ControllerStorage(lifecycle)
      catch {
        case failure: Throwable =>
          try lifecycle.close()
          catch { case closeFailure: Throwable => failure.addSuppressed(closeFailure) }
          throw failure
      }
    }
  }

  def create(config: KafkaConfig,
             time: Time,
             scheduler: Scheduler,
             metadataCache: KRaftMetadataCache,
             metadata: InklessMetadataView,
             metrics: BrokerTopicStats,
             defaultLogConfig: () => LogConfig,
             controlPlane: Option[ControlPlane]): Option[DisklessEngine] = {
    if (!config.disklessStorageSystemEnabled) return None
    val provider: Option[DisklessStorageProvider] =
      if (DisklessEngines.isConfigured(config.originals)) Some(DisklessEngines.load(config.originals))
      else controlPlane.map(cp => nativeProvider(config, cp, metadata, metrics, defaultLogConfig))
    provider.map(_.createBrokerEngine(new DisklessEngineContext(
      DisklessEngines.providerConfigs(config.originals), config.brokerId, time, scheduler,
      () => new KafkaDisklessMetadataSnapshot(metadataCache.currentImage()),
      () => config.extractLogConfigMap)))
  }

  def nativeProvider(config: KafkaConfig,
                     controlPlane: ControlPlane,
                     metadata: InklessMetadataView,
                     metrics: BrokerTopicStats,
                     defaultLogConfig: () => LogConfig): InklessStorageProvider =
    InklessStorageProvider.forBroker(controlPlane, new InklessStorageProvider.BrokerServices(
      config.inklessConfig, metadata, metrics, () => defaultLogConfig(), consolidationConfig(config),
      config.logInitialTaskDelayMs))

  def consolidationConfig(config: KafkaConfig): Optional[InklessStorageProvider.ConsolidationConfig] =
    if (config.disklessRemoteStorageConsolidationEnabled) {
      Optional.of(new InklessStorageProvider.ConsolidationConfig(
        config.disklessConsolidationFetchMetadataThreadPoolSize,
        config.disklessConsolidationFetchDataThreadPoolSize,
        config.disklessConsolidationFetchLaggingRequestRateLimit,
        config.disklessConsolidationFindBatchesMaxPerPartition))
    } else Optional.empty()
}
