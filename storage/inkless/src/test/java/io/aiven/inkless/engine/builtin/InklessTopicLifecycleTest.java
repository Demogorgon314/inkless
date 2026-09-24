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

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;

import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.InMemoryControlPlane;
import io.aiven.inkless.control_plane.ListOffsetsRequest;
import io.aiven.inkless.engine.DisklessTopicLifecycle.PartitionRange;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class InklessTopicLifecycleTest {
    @Test
    void provisionsOnlyRequestedPartitionsAndKeepsTopicIncarnationsSeparate() throws Exception {
        try (var controlPlane = new InMemoryControlPlane(Time.SYSTEM);
             var lifecycle = new InklessTopicLifecycle(controlPlane)) {
            controlPlane.configure(Map.of());
            Uuid oldId = Uuid.randomUuid();
            lifecycle.ensureTopic("topic", oldId, 2).join();
            lifecycle.ensureTopic("topic", oldId, 2).join();
            // Partition 2 represents a migrating partition, initialized separately from its seal.
            lifecycle.ensurePartitions(Set.of(new PartitionRange(oldId, "topic", 3, 4))).join();
            var offsets = controlPlane.listOffsets(List.of(
                latest(oldId, 0), latest(oldId, 1), latest(oldId, 2), latest(oldId, 3)));
            assertEquals(List.of(Errors.NONE, Errors.NONE, Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.NONE),
                offsets.stream().map(r -> r.errors()).toList());

            lifecycle.deleteTopic("topic", oldId).join();
            lifecycle.deleteTopic("topic", oldId).join();
            Uuid replacementId = Uuid.randomUuid();
            lifecycle.ensureTopic("topic", replacementId, 1).join();
            lifecycle.deleteTopic("topic", oldId).join();
            var afterRecreation = controlPlane.listOffsets(List.of(latest(oldId, 0), latest(replacementId, 0)));
            assertEquals(List.of(Errors.UNKNOWN_TOPIC_OR_PARTITION, Errors.NONE),
                afterRecreation.stream().map(r -> r.errors()).toList());
        }
    }

    @Test
    void reportsStorageFailureThroughFutureAndDoesNotCloseBorrowedControlPlane() throws IOException {
        var controlPlane = mock(ControlPlane.class);
        var failure = new IllegalStateException("Storage unavailable");
        doThrow(failure).when(controlPlane).createTopicAndPartitions(any());
        doThrow(failure).when(controlPlane).deleteTopics(any());
        try (var lifecycle = new InklessTopicLifecycle(controlPlane)) {
            Uuid id = Uuid.randomUuid();
            assertSame(failure, assertThrows(CompletionException.class,
                () -> lifecycle.ensureTopic("topic", id, 1).join()).getCause());
            assertSame(failure, assertThrows(CompletionException.class,
                () -> lifecycle.deleteTopic("topic", id).join()).getCause());
        }
        verify(controlPlane, never()).close();
    }

    private static ListOffsetsRequest latest(Uuid topicId, int partition) {
        return new ListOffsetsRequest(new TopicIdPartition(topicId, partition, "topic"),
            ListOffsetsRequest.LATEST_TIMESTAMP);
    }
}
