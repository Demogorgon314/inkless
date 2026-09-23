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

import org.apache.kafka.common.Uuid;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.CreateTopicAndPartitionsRequest;

/** Preserves native provisioning and deletion ordering without claiming revision or deletion fences. */
public final class InklessTopicLifecycle implements DisklessTopicLifecycle.RequestDriven {
    private final ControlPlane controlPlane;

    /** Borrows the control plane; SharedServer retains its lifetime across broker and controller roles. */
    public InklessTopicLifecycle(ControlPlane controlPlane) {
        this.controlPlane = controlPlane;
    }

    @Override
    public CompletableFuture<Void> ensurePartitions(Set<PartitionRange> partitions) {
        return execute(() -> controlPlane.createTopicAndPartitions(partitions.stream()
            .map(p -> new CreateTopicAndPartitionsRequest(
                p.topicId(), p.topicName(), p.firstPartition(), p.partitionLimit()))
            .collect(Collectors.toSet())));
    }

    @Override
    public CompletableFuture<Void> deleteTopic(String topicName, Uuid topicId) {
        return execute(() -> controlPlane.deleteTopics(Set.of(topicId)));
    }

    private static CompletableFuture<Void> execute(Runnable operation) {
        try {
            operation.run();
            return CompletableFuture.completedFuture(null);
        } catch (Exception failure) {
            return CompletableFuture.failedFuture(failure);
        }
    }

    @Override
    public void close() {
        // SharedServer owns the borrowed control plane.
    }
}
