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
import org.apache.kafka.common.protocol.Errors;

import java.util.List;

import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.InitDisklessLogProducerState;
import io.aiven.inkless.control_plane.InitDisklessLogRequest;
import io.aiven.inkless.control_plane.InitDisklessLogResponse;
import io.aiven.inkless.control_plane.RepairDisklessLogRequest;
import io.aiven.inkless.engine.LogTransition;

final class InklessLogTransition implements LogTransition {
    private final ControlPlane controlPlane;

    InklessLogTransition(ControlPlane controlPlane) {
        this.controlPlane = controlPlane;
    }

    @Override
    public List<Errors> initializeLogs(List<LogInitialization> requests) {
        var nativeRequests = requests.stream().map(r -> new InitDisklessLogRequest(
            r.topicId(), r.topicName(), r.partition(), r.logStartOffset(), r.disklessStartOffset(),
            r.producerStates().stream().map(p -> new InitDisklessLogProducerState(
                p.producerId(), p.producerEpoch(), p.baseSequence(), p.lastSequence(),
                p.assignedOffset(), p.batchMaxTimestamp())).toList())).toList();
        var responses = controlPlane.initDisklessLog(nativeRequests);
        return responses == null ? List.of() : responses.stream().map(InklessLogTransition::toTransitionError).toList();
    }

    private static Errors toTransitionError(InitDisklessLogResponse response) {
        return response.equals(InitDisklessLogResponse.alreadyInitialized()) ? Errors.NONE : response.error();
    }

    @Override
    public Errors repairLog(TopicIdPartition partition, long startOffset) {
        var response = controlPlane.repairDisklessLog(List.of(new RepairDisklessLogRequest(
            partition.topicId(), partition.topic(), partition.partition(), startOffset))).get(0);
        return response.found() ? Errors.NONE : Errors.UNKNOWN_TOPIC_OR_PARTITION;
    }

}
