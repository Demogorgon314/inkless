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

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.util.Scheduler;

import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Kafka-owned services supplied to a broker engine.
 *
 * @param configs provider settings with the {@code diskless.engine.config.} prefix removed
 * @param scheduler the broker scheduler; Kafka owns its lifecycle, so the engine only schedules tasks
 * @param metadata captures one committed image per call; call it once per request or maintenance pass
 * @param brokerLogDefaults returns the current broker log defaults, including dynamic updates
 */
public record DisklessEngineContext(Map<String, ?> configs,
                                    int brokerId,
                                    Time time,
                                    Scheduler scheduler,
                                    Supplier<DisklessMetadataSnapshot> metadata,
                                    Supplier<Map<String, ?>> brokerLogDefaults) {
    public DisklessEngineContext {
        configs = Map.copyOf(Objects.requireNonNull(configs, "configs"));
        Objects.requireNonNull(time, "time");
        Objects.requireNonNull(scheduler, "scheduler");
        Objects.requireNonNull(metadata, "metadata");
        Objects.requireNonNull(brokerLogDefaults, "brokerLogDefaults");
    }

    /** Returns a copy that uses the supplied scheduler. */
    public DisklessEngineContext withScheduler(Scheduler replacement) {
        return new DisklessEngineContext(configs, brokerId, time, replacement, metadata, brokerLogDefaults);
    }
}
