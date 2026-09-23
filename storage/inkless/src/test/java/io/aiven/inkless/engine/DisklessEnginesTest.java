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

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import io.aiven.inkless.consume.FetchHandler;
import io.aiven.inkless.consume.FetchOffsetHandler;
import io.aiven.inkless.produce.AppendHandler;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;

public class DisklessEnginesTest {
    @Test
    public void sharesPlatformAndSpiButDoesNotLeakMissingPrivateClassesFromBroker() throws Exception {
        try (var loader = new KafkaPluginClassLoader(new URL[0], getClass().getClassLoader())) {
            assertSame(DisklessEngine.class, loader.loadClass(DisklessEngine.class.getName()));
            assertSame(ClassLoader.getPlatformClassLoader().loadClass("org.w3c.dom.Node"),
                loader.loadClass("org.w3c.dom.Node"));
            assertThrows(ClassNotFoundException.class, () -> loader.loadClass(Test.class.getName()));
        }
    }

    @Test
    public void closesEveryNativeHandlerWhenOneFails() throws IOException {
        var append = mock(AppendHandler.class);
        var fetch = mock(FetchHandler.class);
        var offsets = mock(FetchOffsetHandler.class);
        var failure = new IOException("Append close failed");
        doThrow(failure).when(append).close();
        var engine = new InklessDisklessEngine(append, fetch, offsets);
        assertSame(failure, assertThrows(IOException.class, engine::close));
        verify(fetch).close();
        verify(offsets).close();
    }

    @Test
    public void selectsNativeEngineWhenUnconfigured() throws IOException {
        try (var nativeEngine = new TestEngine()) {
            assertSame(nativeEngine, DisklessEngines.load(Map.of(), () -> nativeEngine));
        }
    }

    @Test
    public void loadsProviderWithoutConstructingNativeEngineOrLeakingBrokerConfig() throws IOException {
        var config = Map.of(
            DisklessEngines.CLASS_NAME_CONFIG, TestEngine.class.getName(),
            DisklessEngines.CONFIG_PREFIX + "endpoint", "test-endpoint",
            "unrelated.broker.setting", "private");
        try (var construction = mockConstruction(TestEngine.class)) {
            try (var loaded = DisklessEngines.load(config, () -> fail("Native engine must stay uninitialized"))) {
                verify(construction.constructed().get(0)).configure(Map.of("endpoint", "test-endpoint"));
            }
            verify(construction.constructed().get(0)).close();
        }
    }

    @Test
    public void closesFailedProviderAndPreservesBothFailures() {
        var configureFailure = new IllegalArgumentException("Configuration rejected");
        var closeFailure = new IOException("Close failed");
        try (var construction = mockConstruction(TestEngine.class, (engine, context) -> {
            doThrow(configureFailure).when(engine).configure(Map.of());
            doThrow(closeFailure).when(engine).close();
        })) {
            var thrown = assertThrows(IllegalArgumentException.class, () -> DisklessEngines.load(
                Map.of(DisklessEngines.CLASS_NAME_CONFIG, TestEngine.class.getName()),
                () -> fail("No fallback on invalid configuration")));
            assertSame(configureFailure, thrown);
            assertSame(closeFailure, thrown.getSuppressed()[0]);
            try {
                verify(construction.constructed().get(0)).close();
            } catch (IOException e) {
                fail(e);
            }
        }
    }

    /** Public no-argument provider used by loader and broker routing tests. */
    public static class TestEngine implements DisklessEngine {
        private Map<String, ?> config;
        private boolean closed;

        @Override
        public void configure(Map<String, ?> configs) {
            config = Map.copyOf(configs);
        }

        @Override
        public CompletableFuture<Map<TopicIdPartition, PartitionResponse>> append(
            Map<TopicIdPartition, MemoryRecords> records, RequestLocal requestLocal) {
            throw new UnsupportedOperationException("Test must supply append behavior");
        }

        @Override
        public CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> fetch(
            FetchParams params, Map<TopicIdPartition, FetchRequest.PartitionData> partitions) {
            throw new UnsupportedOperationException("Test must supply fetch behavior");
        }

        @Override
        public OffsetJob createOffsetJob() {
            throw new UnsupportedOperationException("Test must supply offset behavior");
        }

        @Override
        public void close() throws IOException {
            closed = true;
        }
    }
}
