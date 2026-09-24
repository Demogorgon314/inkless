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

import io.aiven.inkless.control_plane.MetadataView
import io.aiven.inkless.engine.builtin.InklessStorageProvider
import kafka.server.metadata.DisklessTopicView

import java.util.Optional

/** Helpers for broker tests that construct the built-in engine directly. */
object InklessEngineTestSupport {
  /** Lets one mock stand in for both the Kafka routing view and the view the built-in engine reads. */
  trait CombinedTopicView extends DisklessTopicView with MetadataView

  /** Returns the consolidation settings the built-in provider derives from the broker configuration. */
  def consolidationConfig(config: KafkaConfig): Optional[InklessStorageProvider.ConsolidationConfig] =
    if (config.disklessRemoteStorageConsolidationEnabled) {
      Optional.of(new InklessStorageProvider.ConsolidationConfig(
        config.disklessConsolidationFetchMetadataThreadPoolSize,
        config.disklessConsolidationFetchDataThreadPoolSize,
        config.disklessConsolidationFetchLaggingRequestRateLimit,
        config.disklessConsolidationFindBatchesMaxPerPartition))
    } else Optional.empty()
}
