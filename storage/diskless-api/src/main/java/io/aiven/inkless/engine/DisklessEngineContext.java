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
package io.aiven.inkless.engine;

import org.apache.kafka.server.util.Scheduler;

import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Kafka-owned services supplied to a broker engine. Process-level settings arrive earlier, through
 * {@link DisklessStorageProvider#configure(DisklessProviderContext)}.
 *
 * @param scheduler the broker scheduler; Kafka owns its lifecycle, so the engine only schedules tasks
 * @param metadata captures one committed image per call; call it once per request or maintenance pass
 * @param brokerLogDefaults returns the current broker log defaults, including dynamic updates
 * @param metrics the broker topic metrics that the engine reports its request outcomes to
 */
public record DisklessEngineContext(int brokerId,
                                    Scheduler scheduler,
                                    Supplier<DisklessMetadataSnapshot> metadata,
                                    Supplier<Map<String, ?>> brokerLogDefaults,
                                    DisklessTopicMetrics metrics) {
    public DisklessEngineContext {
        Objects.requireNonNull(scheduler, "scheduler");
        Objects.requireNonNull(metadata, "metadata");
        Objects.requireNonNull(brokerLogDefaults, "brokerLogDefaults");
        Objects.requireNonNull(metrics, "metrics");
    }

    /** Returns a copy that uses the supplied scheduler. */
    public DisklessEngineContext withScheduler(Scheduler replacement) {
        return new DisklessEngineContext(brokerId, replacement, metadata, brokerLogDefaults, metrics);
    }
}
