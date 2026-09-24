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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.config.ServerLogConfigs;

import java.util.Optional;

import io.aiven.inkless.common.SharedState;
import io.aiven.inkless.config.InklessConfig;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.engine.DisklessEngine;
import io.aiven.inkless.engine.DisklessEngineContext;
import io.aiven.inkless.engine.DisklessProviderContext;
import io.aiven.inkless.engine.DisklessStorageProvider;
import io.aiven.inkless.engine.DisklessTopicLifecycle;
import io.aiven.inkless.metadata.SnapshotMetadataView;

/** Built-in provider. Reads the {@code inkless.} settings and the Kafka settings that shape its engine. */
public final class InklessStorageProvider implements DisklessStorageProvider {
    /** Consolidation fetch settings; present only when Kafka runs consolidation fetchers. */
    public record ConsolidationConfig(int metadataThreads, int dataThreads,
                                      int requestRateLimit, int maxBatchesPerPartition) { }

    // Kafka validates these settings when it parses the broker configuration.
    private static final ConfigDef KAFKA_SETTINGS = new ConfigDef()
        .define(ServerConfigs.DISKLESS_REMOTE_STORAGE_CONSOLIDATION_ENABLE_CONFIG, ConfigDef.Type.BOOLEAN,
            ServerConfigs.DISKLESS_REMOTE_STORAGE_CONSOLIDATION_ENABLE_DEFAULT, ConfigDef.Importance.LOW, "")
        .define(ServerConfigs.DISKLESS_CONSOLIDATION_FETCH_METADATA_THREAD_POOL_SIZE_CONFIG, ConfigDef.Type.INT,
            ServerConfigs.DISKLESS_CONSOLIDATION_FETCH_METADATA_THREAD_POOL_SIZE_DEFAULT, ConfigDef.Importance.LOW, "")
        .define(ServerConfigs.DISKLESS_CONSOLIDATION_FETCH_DATA_THREAD_POOL_SIZE_CONFIG, ConfigDef.Type.INT,
            ServerConfigs.DISKLESS_CONSOLIDATION_FETCH_DATA_THREAD_POOL_SIZE_DEFAULT, ConfigDef.Importance.LOW, "")
        .define(ServerConfigs.DISKLESS_CONSOLIDATION_FETCH_LAGGING_REQUEST_RATE_LIMIT_CONFIG, ConfigDef.Type.INT,
            ServerConfigs.DISKLESS_CONSOLIDATION_FETCH_LAGGING_REQUEST_RATE_LIMIT_DEFAULT, ConfigDef.Importance.LOW, "")
        .define(ServerConfigs.DISKLESS_CONSOLIDATION_FIND_BATCHES_MAX_PER_PARTITION_CONFIG, ConfigDef.Type.INT,
            ServerConfigs.DISKLESS_CONSOLIDATION_FIND_BATCHES_MAX_PER_PARTITION_DEFAULT, ConfigDef.Importance.LOW, "")
        .define(ServerLogConfigs.LOG_INITIAL_TASK_DELAY_MS_CONFIG, ConfigDef.Type.LONG,
            ServerLogConfigs.LOG_INITIAL_TASK_DELAY_MS_DEFAULT, ConfigDef.Importance.LOW, "");

    private record Configured(InklessConfig config, Time time, ControlPlane controlPlane,
                              Optional<ConsolidationConfig> consolidation, long initialTaskDelayMs) { }

    private final ControlPlane borrowedControlPlane;
    private volatile Configured configured;

    public InklessStorageProvider() {
        this(null);
    }

    private InklessStorageProvider(ControlPlane borrowedControlPlane) {
        this.borrowedControlPlane = borrowedControlPlane;
    }

    /** Returns a provider that uses an existing control plane and leaves it open on close, for tests. */
    public static InklessStorageProvider borrowing(ControlPlane controlPlane) {
        if (controlPlane == null) {
            throw new NullPointerException("controlPlane");
        }
        return new InklessStorageProvider(controlPlane);
    }

    @Override
    public synchronized void configure(DisklessProviderContext context) {
        if (configured != null) {
            throw new IllegalStateException("Provider is already configured");
        }
        var settings = new AbstractConfig(KAFKA_SETTINGS, context.configs(), false);
        var config = new InklessConfig(settings);
        Optional<ConsolidationConfig> consolidation = Optional.empty();
        if (settings.getBoolean(ServerConfigs.DISKLESS_REMOTE_STORAGE_CONSOLIDATION_ENABLE_CONFIG)) {
            consolidation = Optional.of(new ConsolidationConfig(
                settings.getInt(ServerConfigs.DISKLESS_CONSOLIDATION_FETCH_METADATA_THREAD_POOL_SIZE_CONFIG),
                settings.getInt(ServerConfigs.DISKLESS_CONSOLIDATION_FETCH_DATA_THREAD_POOL_SIZE_CONFIG),
                settings.getInt(ServerConfigs.DISKLESS_CONSOLIDATION_FETCH_LAGGING_REQUEST_RATE_LIMIT_CONFIG),
                settings.getInt(ServerConfigs.DISKLESS_CONSOLIDATION_FIND_BATCHES_MAX_PER_PARTITION_CONFIG)));
        }
        var controlPlane = borrowedControlPlane != null
            ? borrowedControlPlane : ControlPlane.create(config, context.time());
        configured = new Configured(config, context.time(), controlPlane, consolidation,
            settings.getLong(ServerLogConfigs.LOG_INITIAL_TASK_DELAY_MS_CONFIG));
    }

    @Override
    public DisklessEngine createBrokerEngine(DisklessEngineContext context) {
        var provider = configured();
        var metadata = new SnapshotMetadataView(context.metadata(), context.brokerLogDefaults());
        var state = SharedState.initialize(provider.time(), context.brokerId(), provider.config(), metadata,
            provider.controlPlane(), context.metrics(), metadata::defaultLogConfig);
        try {
            return new InklessDisklessEngine(state, provider.consolidation(), context.scheduler(),
                provider.initialTaskDelayMs());
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
    public DisklessTopicLifecycle createTopicLifecycle() {
        return new InklessTopicLifecycle(configured().controlPlane());
    }

    @Override
    public synchronized void close() throws Exception {
        var provider = configured;
        if (provider != null && borrowedControlPlane == null) {
            provider.controlPlane().close();
        }
    }

    private Configured configured() {
        var provider = configured;
        if (provider == null) {
            throw new IllegalStateException("Provider is not configured");
        }
        return provider;
    }
}
