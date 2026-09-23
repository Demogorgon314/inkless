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
package kafka.server.metadata;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import io.aiven.inkless.engine.DisklessMetadataSnapshot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaDisklessMetadataSnapshotTest {
    @Test
    void metadataAdvanceCannotStampOldConfigurationWithNewRevision() {
        Uuid id = Uuid.randomUuid();
        var current = new AtomicReference<>(image(id, 1, "1000", 10L, true));
        Supplier<DisklessMetadataSnapshot> snapshots = () -> new KafkaDisklessMetadataSnapshot(current.get());
        var captured = snapshots.get();
        var before = captured.topic(id).orElseThrow();

        // Advance between reads, including another lookup from the captured snapshot.
        current.set(image(id, 2, "2000", 20L, true));
        var sameImage = captured.topic(id).orElseThrow();
        assertEquals(before, sameImage);
        assertEquals(10L, sameImage.sourceRevision());
        assertEquals(1, sameImage.partitionCount());
        assertEquals("1000", sameImage.configs().get(TopicConfig.RETENTION_MS_CONFIG));
        assertTrue(captured.partition(new TopicIdPartition(id, 1, "topic")).isEmpty());

        var after = snapshots.get().topic(id).orElseThrow();
        assertEquals(20L, after.sourceRevision());
        assertEquals(2, after.partitionCount());
        assertEquals("2000", after.configs().get(TopicConfig.RETENTION_MS_CONFIG));
        assertThrows(UnsupportedOperationException.class,
            () -> after.configs().put(TopicConfig.RETENTION_MS_CONFIG, "0"));
    }

    @Test
    void recreationDoesNotResolveOldIdThroughNewTopicName() {
        Uuid oldId = Uuid.randomUuid();
        Uuid newId = Uuid.randomUuid();
        var oldImage = new KafkaDisklessMetadataSnapshot(image(oldId, 1, "1000", 10L, true));
        var replacement = new KafkaDisklessMetadataSnapshot(image(newId, 2, "2000", 20L, true));
        var oldPartition = new TopicIdPartition(oldId, 0, "topic");
        assertTrue(oldImage.partition(oldPartition).isPresent());
        assertTrue(replacement.partition(oldPartition).isEmpty());
        assertTrue(replacement.partition(new TopicIdPartition(newId, 0, "different-name")).isEmpty());
        assertTrue(replacement.partition(new TopicIdPartition(newId, 2, "topic")).isEmpty());
        assertEquals("2000", replacement.partition(new TopicIdPartition(newId, 0, "topic"))
            .orElseThrow().configs().get(TopicConfig.RETENTION_MS_CONFIG));
    }

    @Test
    void classicAndDeletedTopicsAreAbsentFromDisklessSnapshots() {
        Uuid id = Uuid.randomUuid();
        assertTrue(new KafkaDisklessMetadataSnapshot(image(id, 1, "1000", 10L, false)).topic(id).isEmpty());
        assertTrue(new KafkaDisklessMetadataSnapshot(MetadataImage.EMPTY).topic(id).isEmpty());
    }

    private static MetadataImage image(Uuid id, int count, String retention, long revision, boolean diskless) {
        var delta = new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build();
        delta.replay(new TopicRecord().setTopicId(id).setName("topic"));
        for (int partition = 0; partition < count; partition++) {
            delta.replay(new PartitionRecord().setTopicId(id).setPartitionId(partition)
                .setReplicas(List.of(1)).setIsr(List.of(1)).setLeader(1));
        }
        delta.replay(config(TopicConfig.DISKLESS_ENABLE_CONFIG, Boolean.toString(diskless)));
        delta.replay(config(TopicConfig.RETENTION_MS_CONFIG, retention));
        return delta.apply(new MetadataProvenance(revision, 0, 0, true));
    }

    private static ConfigRecord config(String key, String value) {
        return new ConfigRecord().setResourceType(ConfigResource.Type.TOPIC.id()).setResourceName("topic")
            .setName(key).setValue(value);
    }
}
