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

/**
 * Entry point of a diskless storage implementation. Kafka loads every provider, including the
 * built-in one, by class name and creates the broker and controller components through this interface.
 *
 * <p>Kafka creates one provider per process with the public no-argument constructor, calls
 * {@link #configure} once, and shares the provider between the broker and controller roles. The
 * provider may own resources that its components share, such as metadata-store clients.
 * Each component owns its own resources and Kafka closes it independently. Kafka closes the provider
 * after it closes every component the provider created. If construction fails, the provider closes
 * every resource that the failed call opened. No partially initialized broker engine is constructed
 * on a controller.
 *
 * <p>Isolated providers run factory and component calls with the plugin context class loader.
 * They must preserve that context for asynchronous tasks submitted to executors they do not own.
 */
public interface DisklessStorageProvider extends AutoCloseable {
    /** Reads the provider settings and opens shared resources, before Kafka creates any component. */
    void configure(DisklessProviderContext context) throws Exception;

    DisklessEngine createBrokerEngine(DisklessEngineContext context) throws Exception;

    DisklessTopicLifecycle createTopicLifecycle() throws Exception;

    /** Releases resources shared by the provider's components. */
    @Override
    default void close() throws Exception {
    }
}
