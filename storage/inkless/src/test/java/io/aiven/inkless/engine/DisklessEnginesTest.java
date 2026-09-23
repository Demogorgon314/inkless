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
import org.apache.kafka.common.config.ConfigException;
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
import java.util.List;
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
    public static TieredStorage nativeTieredStorage(ControlPlane controlPlane) {
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
        var storage = mock(TieredStorage.class);
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
    public void rejectsMissingProviderConfiguration() {
        assertThrows(ConfigException.class,
            () -> DisklessEngines.loadBroker(Map.of(), null));
    }

    @Test
    public void loadsProviderWithoutConstructingNativeEngineOrLeakingBrokerConfig() throws Exception {
        var config = Map.of(
            DisklessEngines.CLASS_NAME_CONFIG, TestProvider.class.getName(),
            DisklessEngines.CONFIG_PREFIX + "endpoint", "test-endpoint",
            "unrelated.broker.setting", "private");
        var engine = mock(DisklessEngine.class);
        try (var construction = mockConstruction(TestProvider.class, (provider, context) ->
            when(provider.createBrokerEngine(any(), any())).thenReturn(engine))) {
            try (var loaded = DisklessEngines.loadBroker(config, null)) {
                verify(construction.constructed().get(0)).createBrokerEngine(Map.of("endpoint", "test-endpoint"), null);
            }
            verify(engine).close();
        }
    }

    @Test
    public void closesInvalidLifecycleAndPreservesCloseFailure() throws Exception {
        var lifecycle = mock(DisklessTopicLifecycle.class);
        var closeFailure = new IOException("Close failed");
        doThrow(closeFailure).when(lifecycle).close();
        try (var construction = mockConstruction(TestProvider.class, (provider, context) ->
            when(provider.createTopicLifecycle(any())).thenReturn(lifecycle))) {
            var thrown = assertThrows(IllegalArgumentException.class, () -> DisklessEngines.loadLifecycle(
                Map.of(DisklessEngines.CLASS_NAME_CONFIG, TestProvider.class.getName())));
            assertSame(closeFailure, thrown.getSuppressed()[0]);
            verify(lifecycle).close();
            verify(construction.constructed().get(0), never()).createBrokerEngine(any(), any());
        }
    }

    @Test
    public void controllerServiceRetainsTypeContextAndIndependentOwnership() throws Exception {
        var original = Thread.currentThread().getContextClassLoader();
        var lifecycle = mock(DisklessTopicLifecycle.MetadataDriven.class);
        try (var loader = new KafkaPluginClassLoader(new URL[0], getClass().getClassLoader())) {
            var wrapped = DisklessClassLoaderContext.leased(DisklessTopicLifecycle.class, lifecycle,
                DisklessClassLoaderRegistry.acquire(new URL[0], loader));
            var service = (DisklessTopicLifecycle.MetadataDriven) wrapped;
            when(lifecycle.listManagedTopics()).thenAnswer(invocation -> {
                assertSame(loader, Thread.currentThread().getContextClassLoader());
                return CompletableFuture.completedFuture(List.of());
            });
            service.listManagedTopics().join();
            assertSame(original, Thread.currentThread().getContextClassLoader());
            wrapped.close();
            wrapped.close();
            verify(lifecycle).close();
        }
    }

    public static class TestProvider implements DisklessStorageProvider {
        @Override
        public DisklessEngine createBrokerEngine(Map<String, ?> configs, DisklessEngine.Context context) {
            return new TestEngine();
        }

        @Override
        public DisklessTopicLifecycle createTopicLifecycle(Map<String, ?> configs) {
            throw new UnsupportedOperationException("Test must supply controller behavior");
        }
    }

    /** Public no-argument provider used by loader and broker routing tests. */
    public static class TestEngine implements DisklessEngine {
        private boolean closed;

        @Override
        public CompletableFuture<Map<TopicIdPartition, PartitionResponse>> append(
            Map<TopicIdPartition, MemoryRecords> records, RequestLocal requestLocal, DisklessRequestContext requestContext) {
            throw new UnsupportedOperationException("Test must supply append behavior");
        }

        @Override
        public CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> fetch(
            FetchParams params, Map<TopicIdPartition, FetchRequest.PartitionData> partitions) {
            throw new UnsupportedOperationException("Test must supply fetch behavior");
        }

        @Override
        public CompletableFuture<Map<TopicIdPartition, ListOffsetsResult>> listOffsets(Map<TopicIdPartition, ListOffsetsSpec> requests) {
            throw new UnsupportedOperationException("Test must supply offset behavior");
        }

        @Override
        public void close() throws IOException {
            closed = true;
        }
    }
}
