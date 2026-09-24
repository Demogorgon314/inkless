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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsPartitionResult;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.util.Scheduler;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Consumer;

import io.aiven.inkless.common.SharedState;
import io.aiven.inkless.config.InklessConfig;
import io.aiven.inkless.consume.FetchHandler;
import io.aiven.inkless.consume.FetchOffsetHandler;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.CreateTopicAndPartitionsRequest;
import io.aiven.inkless.control_plane.InMemoryControlPlane;
import io.aiven.inkless.control_plane.MetadataView;
import io.aiven.inkless.delete.DeleteRecordsInterceptor;
import io.aiven.inkless.delete.FileCleaner;
import io.aiven.inkless.delete.RetentionEnforcer;
import io.aiven.inkless.delete.TopicPurger;
import io.aiven.inkless.engine.DisklessEngine;
import io.aiven.inkless.engine.DisklessMetadataSnapshot.TopicMetadata;
import io.aiven.inkless.engine.DisklessEngineContractAssertions;
import io.aiven.inkless.engine.DisklessLifecycleContext;
import io.aiven.inkless.engine.DisklessTopicLifecycle;
import io.aiven.inkless.engine.RecordDeletion.DeleteRecordsResult;
import io.aiven.inkless.engine.loader.DisklessEnginesTest;
import io.aiven.inkless.produce.AppendHandler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
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

public class InklessDisklessEngineTest {
    /** Returns a native engine whose handlers are mocks and whose control plane is the supplied one. */
    public static DisklessEngine nativeEngine(ControlPlane controlPlane) {
        return nativeEngine(mock(SharedState.class, RETURNS_DEEP_STUBS), controlPlane);
    }

    /** Returns a native engine with mocked handlers that reads topic metadata from the supplied view. */
    public static DisklessEngine nativeEngine(ControlPlane controlPlane, MetadataView metadata) {
        var state = mock(SharedState.class, RETURNS_DEEP_STUBS);
        when(state.metadata()).thenReturn(metadata);
        return nativeEngine(state, controlPlane);
    }

    private static DisklessEngine nativeEngine(SharedState state, ControlPlane controlPlane) {
        when(state.controlPlane()).thenReturn(controlPlane);
        try (var append = mockConstruction(AppendHandler.class);
             var fetch = mockConstruction(FetchHandler.class);
             var offsets = mockConstruction(FetchOffsetHandler.class);
             var deletes = mockConstruction(DeleteRecordsInterceptor.class);
             var retention = mockConstruction(RetentionEnforcer.class);
             var cleaner = mockConstruction(FileCleaner.class);
             var purger = mockConstruction(TopicPurger.class)) {
            return new InklessDisklessEngine(state, Optional.empty(), mock(Scheduler.class), 0L);
        }
    }

    @Test
    public void exposesEveryNativeExtension() {
        var engine = nativeEngine(mock(ControlPlane.class));
        assertTrue(engine.fetchProbing().isPresent());
        assertTrue(engine.recordDeletion().isPresent());
        assertTrue(engine.logTiering().isPresent());
        assertTrue(engine.logTransition().isPresent());
    }

    @Test
    public void metadataCallbacksKeepTheTopicConfigCacheCurrent() {
        var metadata = mock(MetadataView.class);
        var engine = nativeEngine(mock(ControlPlane.class), metadata);
        var topic = new TopicMetadata(Uuid.randomUuid(), "topic", 1, Map.of("retention.ms", "1000"), 7L);

        engine.onTopicConfigChanged(topic);
        var overrides = new Properties();
        overrides.put("retention.ms", "1000");
        verify(metadata).updateTopicConfig("topic", overrides);

        engine.onTopicDeleted("topic", topic.topicId());
        verify(metadata).removeTopicConfig("topic");

        engine.onBrokerLogDefaultsChanged();
        verify(metadata).reconfigureDefaultLogConfig();
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
            assertThrows(RuntimeException.class,
                () -> new InklessDisklessEngine(state, Optional.empty(), mock(Scheduler.class), 0L));
            verify(append.constructed().get(0)).close();
            verify(fetch.constructed().get(0)).close();
            // The provider still owns state when the engine constructor fails.
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
            var engine = new InklessDisklessEngine(state, Optional.empty(), scheduler, 0L);
            engine.start();
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
            assertThrows(IllegalStateException.class, engine::start);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void deleteRecordsKeepsTopicIdentityAcrossNativeResults() throws Exception {
        var partition = new TopicIdPartition(Uuid.randomUuid(), 0, "topic");
        var missing = new TopicIdPartition(Uuid.randomUuid(), 1, "topic");
        var state = mock(SharedState.class, RETURNS_DEEP_STUBS);
        try (var append = mockConstruction(AppendHandler.class);
             var fetch = mockConstruction(FetchHandler.class);
             var offsets = mockConstruction(FetchOffsetHandler.class);
             var deletes = mockConstruction(DeleteRecordsInterceptor.class, (interceptor, context) ->
                 when(interceptor.intercept(any(), any())).thenAnswer(invocation -> {
                     Map<TopicPartition, Long> requested = invocation.getArgument(0);
                     assertEquals(Map.of(partition.topicPartition(), 10L, missing.topicPartition(), 20L), requested);
                     Consumer<Map<TopicPartition, DeleteRecordsPartitionResult>> callback = invocation.getArgument(1);
                     callback.accept(Map.of(partition.topicPartition(), new DeleteRecordsPartitionResult()
                         .setPartitionIndex(0).setLowWatermark(10L).setErrorCode(Errors.NONE.code())));
                     return true;
                 }));
             var retention = mockConstruction(RetentionEnforcer.class);
             var cleaner = mockConstruction(FileCleaner.class);
             var purger = mockConstruction(TopicPurger.class)) {
            var engine = new InklessDisklessEngine(state, Optional.empty(), mock(Scheduler.class), 0L);
            var results = engine.recordDeletion().orElseThrow()
                .deleteRecords(Map.of(partition, 10L, missing, 20L)).get();
            assertEquals(new DeleteRecordsResult(Errors.NONE, 10L), results.get(partition));
            assertEquals(Errors.UNKNOWN_SERVER_ERROR, results.get(missing).error());
        }
    }

    @Test
    public void nativeOffsetsPreservePartialResultsThroughEngineContract() throws Exception {
        var known = new TopicIdPartition(Uuid.randomUuid(), 0, "topic");
        var missing = new TopicIdPartition(Uuid.randomUuid(), 0, "topic");
        try (var controlPlane = new InMemoryControlPlane(Time.SYSTEM)) {
            controlPlane.configure(Map.of());
            controlPlane.createTopicAndPartitions(Set.of(
                new CreateTopicAndPartitionsRequest(known.topicId(), known.topic(), 0, 1)));
            var state = mock(SharedState.class);
            when(state.time()).thenReturn(Time.SYSTEM);
            when(state.metadata()).thenReturn(mock(MetadataView.class));
            when(state.controlPlane()).thenReturn(controlPlane);
            try (var engine = new InklessDisklessEngine(
                mock(AppendHandler.class), mock(FetchHandler.class), new FetchOffsetHandler(state))) {
                DisklessEngineContractAssertions.assertOffsetBatch(engine, known, missing, 0L);
            }
        }
    }

    @Test
    public void providerWithoutBrokerServicesCreatesOnlyTheLifecycleAndBorrowsTheControlPlane() throws Exception {
        var controlPlane = mock(ControlPlane.class);
        var provider = InklessStorageProvider.borrowing(mock(InklessConfig.class), controlPlane);
        try (var lifecycle = provider.createTopicLifecycle(new DisklessLifecycleContext(Map.of()))) {
            assertInstanceOf(DisklessTopicLifecycle.RequestDriven.class, lifecycle);
        }
        assertThrows(IllegalStateException.class, () -> provider.createBrokerEngine(
            DisklessEnginesTest.testContext(Map.of(), mock(Scheduler.class))));
        provider.close();
        verify(controlPlane, never()).close();
    }
}
