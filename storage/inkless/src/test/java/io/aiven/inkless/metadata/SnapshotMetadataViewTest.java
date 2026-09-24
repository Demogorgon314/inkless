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

import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.engine.DisklessMetadataSnapshot;
import io.aiven.inkless.engine.DisklessMetadataSnapshot.TopicMetadata;
import io.aiven.inkless.engine.builtin.InklessDisklessEngineTest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class SnapshotMetadataViewTest {
    private final Map<String, TopicMetadata> topics = new HashMap<>();
    private final AtomicInteger lookups = new AtomicInteger();
    private Map<String, Object> defaults = new HashMap<>();
    private final SnapshotMetadataView view = new SnapshotMetadataView(Snapshot::new, () -> defaults);

    private final class Snapshot implements DisklessMetadataSnapshot {
        @Override
        public Optional<TopicMetadata> topic(Uuid topicId) {
            return topics.values().stream().filter(topic -> topic.topicId().equals(topicId)).findFirst();
        }

        @Override
        public Optional<TopicMetadata> topic(String name) {
            lookups.incrementAndGet();
            return Optional.ofNullable(topics.get(name));
        }

        @Override
        public Collection<TopicMetadata> topics() {
            return List.copyOf(topics.values());
        }

        @Override
        public int brokerCount() {
            return 3;
        }
    }

    private TopicMetadata publish(String name, int partitions, String... overrides) {
        Map<String, String> configs = new HashMap<>();
        configs.put(TopicConfig.DISKLESS_ENABLE_CONFIG, "true");
        for (int i = 0; i < overrides.length; i += 2) {
            configs.put(overrides[i], overrides[i + 1]);
        }
        var topic = new TopicMetadata(Uuid.randomUuid(), name, partitions, configs, 1L);
        topics.put(name, topic);
        return topic;
    }

    @Test
    void resolvesTopicsFromTheLatestSnapshot() {
        var consolidating = publish("consolidating", 2, TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true");
        var disklessOnly = publish("diskless-only", 1);

        assertEquals(3, view.getBrokerCount());
        assertEquals(consolidating.topicId(), view.getTopicId("consolidating"));
        assertEquals(Uuid.ZERO_UUID, view.getTopicId("missing"));
        assertTrue(view.isDisklessTopic("diskless-only"));
        assertFalse(view.isDisklessTopic("missing"));
        assertTrue(view.isConsolidatingDisklessTopic("consolidating"));
        assertFalse(view.isConsolidatingDisklessTopic("diskless-only"));
        assertEquals(Set.of(
            new TopicIdPartition(consolidating.topicId(), 0, "consolidating"),
            new TopicIdPartition(consolidating.topicId(), 1, "consolidating"),
            new TopicIdPartition(disklessOnly.topicId(), 0, "diskless-only")), view.getDisklessTopicPartitions());
        assertEquals(Set.of(
            new TopicIdPartition(consolidating.topicId(), 0, "consolidating"),
            new TopicIdPartition(consolidating.topicId(), 1, "consolidating")),
            view.getConsolidatingDisklessTopicPartitions());
    }

    @Test
    void mergesBrokerDefaultsWithTopicOverridesAndDropsNullDefaults() {
        defaults.put(TopicConfig.RETENTION_MS_CONFIG, "86400000");
        defaults.put(TopicConfig.RETENTION_BYTES_CONFIG, null);
        defaults.put(TopicConfig.SEGMENT_BYTES_CONFIG, "1073741824");
        publish("topic", 1, TopicConfig.RETENTION_MS_CONFIG, "604800000");

        var config = view.getTopicConfig("topic");

        assertEquals(604800000L, config.retentionMs);
        assertEquals(-1L, config.retentionSize);
        assertEquals(1073741824, config.segmentSize());
    }

    @Test
    void cachesTopicConfigsUntilUpdatedOrRemoved() {
        defaults.put(TopicConfig.RETENTION_MS_CONFIG, "86400000");
        publish("topic", 1);

        var first = view.getTopicConfig("topic");
        assertSame(first, view.getTopicConfig("topic"));
        assertEquals(1, lookups.get());

        var overrides = new Properties();
        overrides.put(TopicConfig.RETENTION_MS_CONFIG, "3600000");
        view.updateTopicConfig("topic", overrides);
        assertEquals(3600000L, view.getTopicConfig("topic").retentionMs);
        assertEquals(1, lookups.get());

        view.removeTopicConfig("topic");
        view.getTopicConfig("topic");
        assertEquals(2, lookups.get());
    }

    @Test
    void updateDoesNotCacheUnreadTopics() {
        publish("unread", 1, TopicConfig.RETENTION_MS_CONFIG, "7200000");
        var overrides = new Properties();
        overrides.put(TopicConfig.RETENTION_MS_CONFIG, "3600000");

        view.updateTopicConfig("unread", overrides);

        assertEquals(7200000L, view.getTopicConfig("unread").retentionMs);
        assertEquals(1, lookups.get());
    }

    @Test
    void reconfiguredDefaultsKeepTopicOverrides() {
        defaults.put(TopicConfig.RETENTION_MS_CONFIG, "86400000");
        defaults.put(TopicConfig.RETENTION_BYTES_CONFIG, "1073741824");
        publish("overridden", 1, TopicConfig.RETENTION_MS_CONFIG, "604800000");
        publish("plain", 1);
        view.getTopicConfig("overridden");
        view.getTopicConfig("plain");

        defaults = new HashMap<>(Map.of(
            TopicConfig.RETENTION_MS_CONFIG, "172800000",
            TopicConfig.RETENTION_BYTES_CONFIG, "536870912"));
        view.reconfigureDefaultLogConfig();

        assertEquals(604800000L, view.getTopicConfig("overridden").retentionMs);
        assertEquals(536870912L, view.getTopicConfig("overridden").retentionSize);
        assertEquals(172800000L, view.getTopicConfig("plain").retentionMs);
        assertEquals(172800000L, view.defaultLogConfig().retentionMs);
    }

    @Test
    void engineCallbacksRefreshTheConfigsThatAppendAndRetentionRead() throws Exception {
        var initial = publish("topic", 1, TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "1048588",
            TopicConfig.RETENTION_MS_CONFIG, "604800000");
        try (var engine = InklessDisklessEngineTest.nativeEngine(mock(ControlPlane.class), view)) {
            assertEquals(1048588, view.getTopicConfig("topic").maxMessageSize());

            var changed = publish("topic", 1, TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "6291456",
                TopicConfig.RETENTION_MS_CONFIG, "3600000");
            engine.onTopicConfigChanged(changed);
            assertEquals(6291456, view.getTopicConfig("topic").maxMessageSize());
            assertEquals(3600000L, view.getTopicConfig("topic").retentionMs);

            engine.onTopicDeleted("topic", initial.topicId());
            topics.remove("topic");
            assertEquals(view.defaultLogConfig().retentionMs, view.getTopicConfig("topic").retentionMs);
        }
    }
}
