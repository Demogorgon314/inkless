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
package io.aiven.inkless.engine.builtin;

import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import io.aiven.inkless.common.SharedState;
import io.aiven.inkless.config.InklessConfig;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.MetadataView;
import io.aiven.inkless.engine.DisklessEngine;
import io.aiven.inkless.engine.DisklessEngineContext;
import io.aiven.inkless.engine.DisklessLifecycleContext;
import io.aiven.inkless.engine.DisklessStorageProvider;
import io.aiven.inkless.engine.DisklessTopicLifecycle;

/**
 * Creates the built-in Inkless components in the broker runtime.
 *
 * <p>The control plane is shared by the broker and controller roles of one process, so the
 * provider borrows it and never closes it. Broker services exist only on brokers: a controller-only
 * provider rejects broker engine creation.
 */
public final class InklessStorageProvider implements DisklessStorageProvider {
    /** Consolidation fetch settings; present only when Kafka runs consolidation fetchers. */
    public record ConsolidationConfig(int metadataThreads, int dataThreads,
                                      int requestRateLimit, int maxBatchesPerPartition) { }

    /** Inkless-specific broker dependencies that the storage-neutral context does not carry. */
    public record BrokerServices(InklessConfig config,
                                 MetadataView metadata,
                                 BrokerTopicStats metrics,
                                 Supplier<LogConfig> defaultLogConfig,
                                 Optional<ConsolidationConfig> consolidation,
                                 long initialTaskDelayMs) {
        public BrokerServices {
            Objects.requireNonNull(config, "config");
            Objects.requireNonNull(metadata, "metadata");
            Objects.requireNonNull(metrics, "metrics");
            Objects.requireNonNull(defaultLogConfig, "defaultLogConfig");
            Objects.requireNonNull(consolidation, "consolidation");
        }
    }

    private final ControlPlane controlPlane;
    private final Optional<BrokerServices> broker;

    private InklessStorageProvider(ControlPlane controlPlane, Optional<BrokerServices> broker) {
        this.controlPlane = Objects.requireNonNull(controlPlane, "controlPlane");
        this.broker = broker;
    }

    public static InklessStorageProvider forBroker(ControlPlane controlPlane, BrokerServices services) {
        return new InklessStorageProvider(controlPlane, Optional.of(services));
    }

    public static InklessStorageProvider forController(ControlPlane controlPlane) {
        return new InklessStorageProvider(controlPlane, Optional.empty());
    }

    @Override
    public DisklessEngine createBrokerEngine(DisklessEngineContext context) {
        var services = broker.orElseThrow(() ->
            new IllegalStateException("Broker services are required to create a broker engine"));
        var state = SharedState.initialize(context.time(), context.brokerId(), services.config(),
            services.metadata(), controlPlane, services.metrics(), services.defaultLogConfig());
        try {
            return new InklessDisklessEngine(state, services.consolidation(), context.scheduler(),
                services.initialTaskDelayMs());
        } catch (RuntimeException | Error failure) {
            try {
                state.close();
            } catch (Exception closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    @Override
    public DisklessTopicLifecycle createTopicLifecycle(DisklessLifecycleContext context) {
        return new InklessTopicLifecycle(controlPlane);
    }
}
