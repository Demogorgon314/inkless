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
 * Entry point of a diskless storage implementation. Kafka creates the broker and controller
 * components through this interface for both the built-in and isolated implementations.
 *
 * <p>Providers have no owned resources; ownership of each component transfers to its caller.
 * If construction fails, the provider closes every resource it opened. No partially initialized
 * broker engine is constructed on a controller.
 *
 * <p>Isolated providers run factory and component calls with the plugin context class loader.
 * They must preserve that context for asynchronous tasks submitted to executors they do not own.
 */
public interface DisklessStorageProvider {
    DisklessEngine createBrokerEngine(DisklessEngineContext context) throws Exception;

    DisklessTopicLifecycle createTopicLifecycle(DisklessLifecycleContext context) throws Exception;
}
