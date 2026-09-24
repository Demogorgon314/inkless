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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.ChildFirstClassLoader;
import org.apache.kafka.common.utils.Utils;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import io.aiven.inkless.engine.DisklessEngine;
import io.aiven.inkless.engine.DisklessEngineContext;
import io.aiven.inkless.engine.DisklessLifecycleContext;
import io.aiven.inkless.engine.DisklessStorageProvider;
import io.aiven.inkless.engine.DisklessTopicLifecycle;

/** Loads storage providers with shared Kafka APIs and an isolated provider dependency runtime. */
public final class DisklessEngines {
    public static final String CLASS_NAME_CONFIG = "diskless.engine.class.name";
    public static final String CLASS_PATH_CONFIG = "diskless.engine.class.path";
    public static final String CONFIG_PREFIX = "diskless.engine.config.";

    private DisklessEngines() {
    }

    /** Checks whether the broker configuration selects an external storage provider. */
    public static boolean isConfigured(Map<String, ?> configs) {
        return configs.containsKey(CLASS_NAME_CONFIG);
    }

    /**
     * Returns a provider whose components run in the configured plugin runtime. Each created
     * component holds its own runtime lease and releases it on close.
     */
    public static DisklessStorageProvider load(Map<String, ?> configs) {
        Object className = configs.get(CLASS_NAME_CONFIG);
        if (className == null) {
            throw new ConfigException(CLASS_NAME_CONFIG, null, "A storage provider class is required");
        }
        Object classPath = configs.get(CLASS_PATH_CONFIG);
        URL[] urls = new URL[0];
        if (classPath != null) {
            try (var paths = new ChildFirstClassLoader(classPath.toString(), DisklessEngine.class.getClassLoader())) {
                urls = paths.getURLs();
            } catch (Exception e) {
                throw new ConfigException(CLASS_PATH_CONFIG, classPath, "Cannot resolve plugin files: " + e.getMessage());
            }
            if (urls.length == 0) {
                throw new ConfigException(CLASS_PATH_CONFIG, classPath, "No plugin files found");
            }
        }
        return new IsolatedProvider(className.toString(), urls);
    }

    /** Returns the provider settings with the {@code diskless.engine.config.} prefix removed. */
    public static Map<String, Object> providerConfigs(Map<String, ?> configs) {
        Map<String, Object> properties = new HashMap<>();
        configs.forEach((key, value) -> {
            if (key.startsWith(CONFIG_PREFIX)) {
                properties.put(key.substring(CONFIG_PREFIX.length()), value);
            }
        });
        return Map.copyOf(properties);
    }

    private record IsolatedProvider(String className, URL[] urls) implements DisklessStorageProvider {
        @Override
        public DisklessEngine createBrokerEngine(DisklessEngineContext context) {
            return create(DisklessEngine.class, (provider, classLoader) -> provider.createBrokerEngine(
                context.withScheduler(DisklessClassLoaderContext.scheduler(context.scheduler(), classLoader))));
        }

        @Override
        public DisklessTopicLifecycle createTopicLifecycle(DisklessLifecycleContext context) {
            return create(DisklessTopicLifecycle.class, (provider, classLoader) -> provider.createTopicLifecycle(context));
        }

        private <T> T create(Class<T> type, ComponentFactory<T> factory) {
            DisklessClassLoaderRegistry.Lease lease = null;
            try {
                lease = DisklessClassLoaderRegistry.acquire(urls, DisklessEngine.class.getClassLoader());
                var classLoader = lease.classLoader();
                T component = DisklessClassLoaderContext.call(classLoader, () -> {
                    DisklessStorageProvider provider = Utils.newInstance(
                        Class.forName(className, true, classLoader).asSubclass(DisklessStorageProvider.class));
                    return factory.create(provider, classLoader);
                });
                try {
                    return DisklessClassLoaderContext.leased(type, component, lease);
                } catch (RuntimeException | Error failure) {
                    try {
                        DisklessClassLoaderContext.call(classLoader, () -> {
                            ((AutoCloseable) component).close();
                            return null;
                        });
                    } catch (Exception | Error closeFailure) {
                        failure.addSuppressed(closeFailure);
                    }
                    throw failure;
                }
            } catch (Throwable e) {
                if (lease != null) {
                    DisklessClassLoaderRegistry.closeLeaseOnFailure(lease, e);
                }
                if (e instanceof RuntimeException runtimeException) {
                    throw runtimeException;
                }
                if (e instanceof Error error) {
                    throw error;
                }
                throw new KafkaException("Failed to create diskless component from " + className, e);
            }
        }
    }

    @FunctionalInterface
    private interface ComponentFactory<T> {
        T create(DisklessStorageProvider provider, ClassLoader classLoader) throws Exception;
    }
}
