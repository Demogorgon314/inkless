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
import org.apache.kafka.server.util.Scheduler;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URL;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;

import io.aiven.inkless.common.SharedState;
import io.aiven.inkless.consume.FetchHandler;
import io.aiven.inkless.consume.FetchOffsetHandler;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.delete.DeleteRecordsInterceptor;
import io.aiven.inkless.delete.FileCleaner;
import io.aiven.inkless.delete.RetentionEnforcer;
import io.aiven.inkless.delete.TopicPurger;
import io.aiven.inkless.produce.AppendHandler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DisklessEnginesTest {
    public static DisklessEngine.TieredStorage nativeTieredStorage(ControlPlane controlPlane) {
        var state = mock(SharedState.class);
        when(state.controlPlane()).thenReturn(controlPlane);
        return new InklessTieredStorage(state, Optional.empty());
    }

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
    public void constructionFailureClosesPreviouslyCreatedHandlers() throws IOException {
        var state = mock(SharedState.class);
        try (var append = mockConstruction(AppendHandler.class);
             var fetch = mockConstruction(FetchHandler.class);
             var offsets = mockConstruction(FetchOffsetHandler.class, (handler, context) -> {
                 throw new IllegalStateException("Cannot create offset handler");
             })) {
            assertThrows(RuntimeException.class, () -> new InklessDisklessEngine(state));
            verify(append.constructed().get(0)).close();
            verify(fetch.constructed().get(0)).close();
            // The factory still owns state when the engine constructor fails.
            verify(state, never()).close();
            assertEquals(0, offsets.constructed().size());
        }
    }

    @Test
    public void closesMaintenanceAndStateEvenWhenAHandlerFails() throws IOException {
        var state = mock(SharedState.class, RETURNS_DEEP_STUBS);
        when(state.config().fileCleanerInterval()).thenReturn(Duration.ofSeconds(1));
        when(state.config().topicPurgerInterval()).thenReturn(Duration.ofSeconds(1));
        when(state.config().crossTierLogStartReportInterval()).thenReturn(Duration.ofSeconds(1));
        var scheduler = mock(Scheduler.class);
        var task = mock(ScheduledFuture.class);
        doReturn(task).when(scheduler).schedule(anyString(), any(), anyLong(), anyLong());
        try (var append = mockConstruction(AppendHandler.class);
             var fetch = mockConstruction(FetchHandler.class);
             var offsets = mockConstruction(FetchOffsetHandler.class);
             var deletes = mockConstruction(DeleteRecordsInterceptor.class);
             var retention = mockConstruction(RetentionEnforcer.class);
             var cleaner = mockConstruction(FileCleaner.class);
             var purger = mockConstruction(TopicPurger.class)) {
            var engine = new InklessDisklessEngine(state);
            engine.start(scheduler, 0L);
            var failure = new IOException("Append close failed");
            doThrow(failure).when(append.constructed().get(0)).close();
            assertSame(failure, assertThrows(IOException.class, engine::close));
            verify(task, times(4)).cancel(false);
            verify(fetch.constructed().get(0)).close();
            verify(offsets.constructed().get(0)).close();
            verify(deletes.constructed().get(0)).close();
            verify(retention.constructed().get(0)).close();
            verify(cleaner.constructed().get(0)).close();
            verify(purger.constructed().get(0)).close();
            verify(state).close();
            engine.close();
            verify(state).close();
            assertThrows(IllegalStateException.class, () -> engine.start(scheduler, 0L));
        }
    }

    @Test
    public void tieredStorageCallsUsePluginContextAndRestoreItAfterFailure() throws Exception {
        var original = Thread.currentThread().getContextClassLoader();
        var delegate = mock(DisklessEngine.class);
        var storage = mock(DisklessEngine.TieredStorage.class);
        when(delegate.tieredStorage()).thenReturn(Optional.of(storage));
        try (var loader = new KafkaPluginClassLoader(new URL[0], getClass().getClassLoader());
             var engine = DisklessClassLoaderContext.leased(DisklessEngine.class, delegate,
                 DisklessClassLoaderRegistry.acquire(new URL[0], loader))) {
            when(storage.cleanupIntervalMs()).thenAnswer(invocation -> {
                assertSame(loader, Thread.currentThread().getContextClassLoader());
                return 100L;
            });
            when(storage.repairLog(any(), anyLong())).thenAnswer(invocation -> {
                assertSame(loader, Thread.currentThread().getContextClassLoader());
                throw new IllegalStateException("Storage unavailable");
            });
            var capability = engine.tieredStorage().orElseThrow();
            assertEquals(100L, capability.cleanupIntervalMs());
            assertSame(original, Thread.currentThread().getContextClassLoader());
            assertThrows(IllegalStateException.class, () -> capability.repairLog(null, 0L));
            assertSame(original, Thread.currentThread().getContextClassLoader());
        }
        verify(delegate).close();
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
