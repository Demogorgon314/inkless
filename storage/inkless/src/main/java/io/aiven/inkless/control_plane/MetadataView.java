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
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.storage.internals.log.LogConfig;

import java.util.Properties;
import java.util.Set;

public interface MetadataView {
    Integer getBrokerCount();

    Uuid getTopicId(String topicName);

    boolean isDisklessTopic(String topicName);

    boolean isRemoteStorageEnabled(String topicName);

    boolean isConsolidatingDisklessTopic(String topicName);

    LogConfig getTopicConfig(String topicName);

    /** Replaces the cached configuration of a topic with the committed overrides. */
    void updateTopicConfig(String topicName, Properties topicOverrides);

    /** Drops the cached configuration of a deleted topic. */
    void removeTopicConfig(String topicName);

    /** Rebuilds cached configurations after the broker log defaults change. */
    void reconfigureDefaultLogConfig();

    Set<TopicIdPartition> getDisklessTopicPartitions();

    Set<TopicIdPartition> getConsolidatingDisklessTopicPartitions();
}
