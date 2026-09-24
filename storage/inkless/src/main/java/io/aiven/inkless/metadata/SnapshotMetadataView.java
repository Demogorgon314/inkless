/*
 * Inkless
 * Copyright (C) 2024 - 2025 Aiven OY
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.aiven.inkless.metadata;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.storage.internals.log.LogConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.aiven.inkless.control_plane.MetadataView;
import io.aiven.inkless.engine.DisklessMetadataSnapshot;
import io.aiven.inkless.engine.DisklessMetadataSnapshot.TopicMetadata;

/**
 * Adapts the engine SPI metadata snapshots to the view that the Inkless components use.
 * Every lookup reads the latest committed image; only log configurations are cached.
 */
public final class SnapshotMetadataView implements MetadataView {
    private final Supplier<DisklessMetadataSnapshot> snapshots;
    private final Supplier<Map<String, ?>> brokerLogDefaults;

    // Building a LogConfig parses and validates every log setting, so produce requests reuse the
    // cached one. The engine callbacks keep the cache current, as LocalLog does for classic topics.
    private final ConcurrentHashMap<String, LogConfig> topicConfigs = new ConcurrentHashMap<>();
    private volatile LogConfig defaultConfig;

    public SnapshotMetadataView(Supplier<DisklessMetadataSnapshot> snapshots, Supplier<Map<String, ?>> brokerLogDefaults) {
        this.snapshots = Objects.requireNonNull(snapshots, "snapshots");
        this.brokerLogDefaults = Objects.requireNonNull(brokerLogDefaults, "brokerLogDefaults");
    }

    @Override
    public Integer getBrokerCount() {
        return snapshots.get().brokerCount();
    }

    @Override
    public Uuid getTopicId(String topicName) {
        return snapshots.get().topic(topicName).map(TopicMetadata::topicId).orElse(Uuid.ZERO_UUID);
    }

    @Override
    public boolean isDisklessTopic(String topicName) {
        return snapshots.get().topic(topicName).isPresent();
    }

    @Override
    public boolean isRemoteStorageEnabled(String topicName) {
        return snapshots.get().topic(topicName).filter(SnapshotMetadataView::remoteStorageEnabled).isPresent();
    }

    @Override
    public boolean isConsolidatingDisklessTopic(String topicName) {
        return isRemoteStorageEnabled(topicName);
    }

    @Override
    public LogConfig getTopicConfig(String topicName) {
        return topicConfigs.computeIfAbsent(topicName, name -> snapshots.get().topic(name)
            .map(topic -> LogConfig.fromProps(defaults(), overrides(topic.configs())))
            .orElseGet(this::defaultLogConfig));
    }

    /** Returns the log configuration built from the current broker log defaults. */
    public LogConfig defaultLogConfig() {
        LogConfig config = defaultConfig;
        if (config == null) {
            config = new LogConfig(defaults());
            defaultConfig = config;
        }
        return config;
    }

    @Override
    public void updateTopicConfig(String topicName, Properties topicOverrides) {
        topicConfigs.computeIfPresent(topicName, (name, existing) -> LogConfig.fromProps(defaults(), topicOverrides));
    }

    @Override
    public void removeTopicConfig(String topicName) {
        topicConfigs.remove(topicName);
    }

    @Override
    public void reconfigureDefaultLogConfig() {
        Map<String, Object> newDefaults = defaults();
        defaultConfig = new LogConfig(newDefaults);
        topicConfigs.replaceAll((name, existing) -> {
            Map<String, Object> props = new HashMap<>(newDefaults);
            existing.originals().forEach((key, value) -> {
                if (existing.overriddenConfigs.contains(key)) {
                    props.put(key, value);
                }
            });
            return new LogConfig(props, existing.overriddenConfigs);
        });
    }

    @Override
    public Set<TopicIdPartition> getDisklessTopicPartitions() {
        return partitions(topic -> true);
    }

    @Override
    public Set<TopicIdPartition> getConsolidatingDisklessTopicPartitions() {
        return partitions(SnapshotMetadataView::remoteStorageEnabled);
    }

    private Set<TopicIdPartition> partitions(Predicate<TopicMetadata> filter) {
        return snapshots.get().topics().stream()
            .filter(filter)
            .flatMap(topic -> IntStream.range(0, topic.partitionCount())
                .mapToObj(partition -> new TopicIdPartition(topic.topicId(), partition, topic.name())))
            .collect(Collectors.toSet());
    }

    private Map<String, Object> defaults() {
        // Null values break LogConfig initialization through Properties.putAll.
        Map<String, Object> defaults = new HashMap<>();
        brokerLogDefaults.get().forEach((key, value) -> {
            if (value != null) {
                defaults.put(key, value);
            }
        });
        return defaults;
    }

    private static Properties overrides(Map<String, String> configs) {
        Properties properties = new Properties();
        properties.putAll(configs);
        return properties;
    }

    private static boolean remoteStorageEnabled(TopicMetadata topic) {
        return Boolean.parseBoolean(topic.configs().get(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG));
    }
}
