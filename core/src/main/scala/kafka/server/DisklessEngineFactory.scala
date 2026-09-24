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

import io.aiven.inkless.engine.{DisklessEngine, DisklessEngineContext, DisklessStorageProvider, DisklessTopicLifecycle}
import kafka.server.diskless.DisklessEngines
import kafka.server.metadata.KafkaDisklessMetadataSnapshot
import org.apache.kafka.common.utils.Time
import org.apache.kafka.metadata.KRaftMetadataCache
import org.apache.kafka.server.util.Scheduler
import org.apache.kafka.storage.log.metrics.BrokerTopicStats

/**
 * Loads the process-scoped storage provider and creates its broker and controller components.
 * Every provider, including the built-in one, goes through the same SPI.
 */
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

  /** Creates the provider shared by the broker and controller roles of this process. */
  def createProvider(config: KafkaConfig, time: Time): Option[DisklessStorageProvider] =
    if (!config.disklessStorageSystemEnabled) None
    else Some(DisklessEngines.load(config.originals, time))

  def createControllerStorage(provider: Option[DisklessStorageProvider]): Option[ControllerStorage] = {
    provider.map { p =>
      val lifecycle = p.createTopicLifecycle()
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
             scheduler: Scheduler,
             metadataCache: KRaftMetadataCache,
             metrics: BrokerTopicStats,
             provider: Option[DisklessStorageProvider]): Option[DisklessEngine] = {
    provider.map(_.createBrokerEngine(new DisklessEngineContext(
      config.brokerId, scheduler,
      () => new KafkaDisklessMetadataSnapshot(metadataCache.currentImage()),
      () => config.extractLogConfigMap,
      new DisklessBrokerTopicMetrics(metrics))))
  }
}
