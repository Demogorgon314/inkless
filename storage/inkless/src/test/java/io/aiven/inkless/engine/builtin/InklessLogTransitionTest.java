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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Errors;

import org.junit.jupiter.api.Test;

import java.util.List;

import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.InitDisklessLogResponse;
import io.aiven.inkless.engine.LogTransition.LogInitialization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class InklessLogTransitionTest {
    private static LogInitialization initialization(int partition) {
        return new LogInitialization(Uuid.randomUuid(), "topic", partition, 0L, 10L, List.of());
    }

    @Test
    void reportsAlreadyInitializedLogsAsInitialized() {
        var controlPlane = mock(ControlPlane.class);
        when(controlPlane.initDisklessLog(any())).thenReturn(List.of(
            InitDisklessLogResponse.success(),
            InitDisklessLogResponse.alreadyInitialized(),
            new InitDisklessLogResponse(Errors.UNKNOWN_SERVER_ERROR)));

        var errors = new InklessLogTransition(controlPlane)
            .initializeLogs(List.of(initialization(0), initialization(1), initialization(2)));

        assertEquals(List.of(Errors.NONE, Errors.NONE, Errors.UNKNOWN_SERVER_ERROR), errors);
    }
}
