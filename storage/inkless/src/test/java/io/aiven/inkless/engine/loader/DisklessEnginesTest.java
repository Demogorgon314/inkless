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
package io.aiven.inkless.engine.loader;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.server.util.Scheduler;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import io.aiven.inkless.engine.DisklessEngine;
import io.aiven.inkless.engine.DisklessEngineContext;
import io.aiven.inkless.engine.DisklessLifecycleContext;
import io.aiven.inkless.engine.DisklessRequestContext;
import io.aiven.inkless.engine.DisklessStorageProvider;
import io.aiven.inkless.engine.DisklessTopicLifecycle;
import io.aiven.inkless.engine.FetchProbing;
import io.aiven.inkless.engine.LogTiering;
import io.aiven.inkless.engine.LogTransition;
import io.aiven.inkless.engine.RecordDeletion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DisklessEnginesTest {
    /** Returns a broker context with placeholder services for loader and routing tests. */
    public static DisklessEngineContext testContext(Map<String, ?> configs, Scheduler scheduler) {
        return new DisklessEngineContext(configs, 0, Time.SYSTEM, scheduler,
            () -> topicId -> Optional.empty(), Map::of);
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
    public void extensionCallsUsePluginContextAndRestoreItAfterFailure() throws Exception {
        var original = Thread.currentThread().getContextClassLoader();
        var delegate = mock(DisklessEngine.class);
        var transition = mock(LogTransition.class);
        when(delegate.logTransition()).thenReturn(Optional.of(transition));
        try (var loader = new KafkaPluginClassLoader(new URL[0], getClass().getClassLoader());
             var engine = DisklessClassLoaderContext.leased(DisklessEngine.class, delegate,
                 DisklessClassLoaderRegistry.acquire(new URL[0], loader))) {
            when(transition.initializeLogs(any())).thenAnswer(invocation -> {
                assertSame(loader, Thread.currentThread().getContextClassLoader());
                return List.of(Errors.NONE);
            });
            when(transition.repairLog(any(), anyLong())).thenAnswer(invocation -> {
                assertSame(loader, Thread.currentThread().getContextClassLoader());
                throw new IllegalStateException("Storage unavailable");
            });
            var wrapped = engine.logTransition().orElseThrow();
            assertNotSame(transition, wrapped);
            assertEquals(List.of(Errors.NONE), wrapped.initializeLogs(List.of()));
            assertSame(original, Thread.currentThread().getContextClassLoader());
            assertThrows(IllegalStateException.class, () -> wrapped.repairLog(null, 0L));
            assertSame(original, Thread.currentThread().getContextClassLoader());
            assertTrue(engine.logTiering().isEmpty());
        }
        verify(delegate).close();
    }

    @Test
    public void everyEngineOperationUsesAndRestoresPluginContext() throws Exception {
        for (var method : DisklessEngine.class.getMethods()) {
            if (!method.getName().equals("close")) {
                assertOperationUsesPluginContext(DisklessEngine.class, method);
            }
        }
    }

    @Test
    public void everyExtensionOperationUsesAndRestoresPluginContext() throws Exception {
        for (Class<?> extension : List.of(FetchProbing.class, RecordDeletion.class, LogTiering.class, LogTransition.class)) {
            for (var method : extension.getMethods()) {
                assertOperationUsesPluginContext(extension, method);
            }
        }
    }

    private <T> void assertOperationUsesPluginContext(Class<T> type, Method method) throws Exception {
        var original = Thread.currentThread().getContextClassLoader();
        var failure = new IllegalStateException("Engine operation failure");
        try (var loader = new KafkaPluginClassLoader(new URL[0], getClass().getClassLoader())) {
            T delegate = mock(type, invocation -> {
                if (invocation.getMethod().equals(method)) {
                    assertSame(loader, Thread.currentThread().getContextClassLoader(), method.getName());
                    throw failure;
                }
                return null;
            });
            var engine = mock(DisklessEngine.class);
            var lease = DisklessClassLoaderRegistry.acquire(new URL[0], loader);
            Object target;
            if (type == DisklessEngine.class) {
                target = DisklessClassLoaderContext.leased(DisklessEngine.class, (DisklessEngine) delegate, lease);
            } else {
                when(engine.fetchProbing()).thenReturn(Optional.of(delegate).filter(FetchProbing.class::isInstance).map(FetchProbing.class::cast));
                when(engine.recordDeletion()).thenReturn(Optional.of(delegate).filter(RecordDeletion.class::isInstance).map(RecordDeletion.class::cast));
                when(engine.logTiering()).thenReturn(Optional.of(delegate).filter(LogTiering.class::isInstance).map(LogTiering.class::cast));
                when(engine.logTransition()).thenReturn(Optional.of(delegate).filter(LogTransition.class::isInstance).map(LogTransition.class::cast));
                var leased = DisklessClassLoaderContext.leased(DisklessEngine.class, engine, lease);
                target = List.of(leased.fetchProbing(), leased.recordDeletion(), leased.logTiering(), leased.logTransition())
                    .stream().flatMap(Optional::stream).findFirst().orElseThrow();
            }
            Object[] arguments = Arrays.stream(method.getParameterTypes())
                .map(parameter -> Array.get(Array.newInstance(parameter, 1), 0)).toArray();
            var thrown = assertThrows(InvocationTargetException.class,
                () -> method.invoke(target, arguments), method.getName());
            assertSame(failure, thrown.getCause());
            assertSame(original, Thread.currentThread().getContextClassLoader());
            lease.close();
        }
    }

    @Test
    public void rejectsMissingProviderConfiguration() {
        assertThrows(ConfigException.class, () -> DisklessEngines.load(Map.of()));
    }

    @Test
    public void providerConfigsDropBrokerSettings() {
        var config = Map.of(
            DisklessEngines.CLASS_NAME_CONFIG, TestProvider.class.getName(),
            DisklessEngines.CONFIG_PREFIX + "endpoint", "test-endpoint",
            "unrelated.broker.setting", "private");
        assertEquals(Map.of("endpoint", "test-endpoint"), DisklessEngines.providerConfigs(config));
    }

    @Test
    public void loadedEngineReceivesPluginSchedulerAndOwnsItsRuntime() throws Exception {
        var scheduler = mock(Scheduler.class);
        var context = testContext(Map.of("endpoint", "test-endpoint"), scheduler);
        var engine = mock(DisklessEngine.class);
        try (var construction = mockConstruction(TestProvider.class, (provider, ignored) ->
            when(provider.createBrokerEngine(any())).thenReturn(engine))) {
            var provider = DisklessEngines.load(Map.of(DisklessEngines.CLASS_NAME_CONFIG, TestProvider.class.getName()));
            try (var loaded = provider.createBrokerEngine(context)) {
                var captured = ArgumentCaptor.forClass(DisklessEngineContext.class);
                verify(construction.constructed().get(0)).createBrokerEngine(captured.capture());
                assertEquals(context.configs(), captured.getValue().configs());
                assertNotSame(scheduler, captured.getValue().scheduler());
                assertInstanceOf(DisklessEngine.class, loaded);
            }
            verify(engine).close();
        }
    }

    @Test
    public void pluginSchedulerRunsTasksInPluginContextWithoutLifecycleControl() throws Exception {
        var original = Thread.currentThread().getContextClassLoader();
        var scheduler = mock(Scheduler.class);
        try (var loader = new KafkaPluginClassLoader(new URL[0], getClass().getClassLoader())) {
            var wrapped = DisklessClassLoaderContext.scheduler(scheduler, loader);
            var observed = new AtomicReference<ClassLoader>();
            wrapped.schedule("task", () -> observed.set(Thread.currentThread().getContextClassLoader()), 1L, 2L);
            var task = ArgumentCaptor.forClass(Runnable.class);
            verify(scheduler).schedule(eq("task"), task.capture(), eq(1L), eq(2L));
            task.getValue().run();
            assertSame(loader, observed.get());
            assertSame(original, Thread.currentThread().getContextClassLoader());
            assertThrows(UnsupportedOperationException.class, wrapped::startup);
            assertThrows(UnsupportedOperationException.class, wrapped::shutdown);
            assertThrows(UnsupportedOperationException.class, () -> wrapped.resizeThreadPool(2));
        }
    }

    @Test
    public void controllerServiceRetainsTypeContextAndIndependentOwnership() throws Exception {
        var original = Thread.currentThread().getContextClassLoader();
        var lifecycle = mock(DisklessTopicLifecycle.MetadataDriven.class);
        try (var loader = new KafkaPluginClassLoader(new URL[0], getClass().getClassLoader())) {
            var wrapped = DisklessClassLoaderContext.leased(DisklessTopicLifecycle.class, lifecycle,
                DisklessClassLoaderRegistry.acquire(new URL[0], loader));
            var service = assertInstanceOf(DisklessTopicLifecycle.MetadataDriven.class, wrapped);
            assertTrue(!(wrapped instanceof DisklessTopicLifecycle.RequestDriven));
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
        public DisklessEngine createBrokerEngine(DisklessEngineContext context) {
            return new TestEngine();
        }

        @Override
        public DisklessTopicLifecycle createTopicLifecycle(DisklessLifecycleContext context) {
            throw new UnsupportedOperationException("Test must supply controller behavior");
        }
    }

    /** Public no-argument engine used by loader and broker routing tests. */
    public static class TestEngine implements DisklessEngine {
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
        }
    }
}
