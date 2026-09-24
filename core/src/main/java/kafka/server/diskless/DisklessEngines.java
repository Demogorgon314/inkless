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
package kafka.server.diskless;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.ChildFirstClassLoader;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import java.net.URL;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import io.aiven.inkless.engine.DisklessEngine;
import io.aiven.inkless.engine.DisklessEngineContext;
import io.aiven.inkless.engine.DisklessProviderContext;
import io.aiven.inkless.engine.DisklessStorageProvider;
import io.aiven.inkless.engine.DisklessTopicLifecycle;

/**
 * Loads the configured storage provider and configures it. Without a class path, the provider shares
 * the broker class loader; with one, it runs in an isolated dependency runtime behind SPI proxies.
 */
public final class DisklessEngines {
    public static final String CLASS_NAME_CONFIG = "diskless.engine.class.name";
    public static final String CLASS_PATH_CONFIG = "diskless.engine.class.path";
    public static final String BUILT_IN_CLASS_NAME = "io.aiven.inkless.engine.builtin.InklessStorageProvider";

    private DisklessEngines() {
    }

    /** Checks whether the broker configuration selects a provider other than the built-in one. */
    public static boolean isExternal(Map<String, ?> configs) {
        return !BUILT_IN_CLASS_NAME.equals(className(configs));
    }

    /**
     * Returns the configured process-scoped provider. An isolated provider holds a runtime lease until
     * it is closed, and each component it creates holds its own lease.
     */
    public static DisklessStorageProvider load(Map<String, ?> configs, Time time) {
        String className = className(configs);
        Object classPath = configs.get(CLASS_PATH_CONFIG);
        DisklessStorageProvider provider = classPath == null ? loadShared(className) : loadIsolated(className, classPath);
        try {
            provider.configure(new DisklessProviderContext(configs, time));
            return provider;
        } catch (Throwable e) {
            try {
                provider.close();
            } catch (Throwable closeFailure) {
                e.addSuppressed(closeFailure);
            }
            throw propagate(className, e);
        }
    }

    private static String className(Map<String, ?> configs) {
        Object className = configs.get(CLASS_NAME_CONFIG);
        return className == null ? BUILT_IN_CLASS_NAME : className.toString();
    }

    private static DisklessStorageProvider loadShared(String className) {
        try {
            return Utils.newInstance(className, DisklessStorageProvider.class);
        } catch (ClassNotFoundException e) {
            throw new ConfigException(CLASS_NAME_CONFIG, className, "Storage provider class not found");
        }
    }

    private static DisklessStorageProvider loadIsolated(String className, Object classPath) {
        URL[] urls;
        try (var paths = new ChildFirstClassLoader(classPath.toString(), DisklessEngine.class.getClassLoader())) {
            urls = paths.getURLs();
        } catch (Exception e) {
            throw new ConfigException(CLASS_PATH_CONFIG, classPath, "Cannot resolve plugin files: " + e.getMessage());
        }
        if (urls.length == 0) {
            throw new ConfigException(CLASS_PATH_CONFIG, classPath, "No plugin files found");
        }
        return loadIsolated(className, urls);
    }

    // Visible for testing: an empty URL array isolates the provider behind proxies on the broker loader.
    static DisklessStorageProvider loadIsolated(String className, URL[] urls) {
        DisklessClassLoaderRegistry.Lease lease = null;
        try {
            lease = DisklessClassLoaderRegistry.acquire(urls, DisklessEngine.class.getClassLoader());
            var classLoader = lease.classLoader();
            DisklessStorageProvider provider = DisklessClassLoaderContext.call(classLoader, () -> Utils.newInstance(
                Class.forName(className, true, classLoader).asSubclass(DisklessStorageProvider.class)));
            return new IsolatedProvider(className, urls, provider, lease);
        } catch (Throwable e) {
            if (lease != null) {
                DisklessClassLoaderRegistry.closeLeaseOnFailure(lease, e);
            }
            throw propagate(className, e);
        }
    }

    private static RuntimeException propagate(String className, Throwable e) {
        if (e instanceof RuntimeException runtimeException) {
            return runtimeException;
        }
        if (e instanceof Error error) {
            throw error;
        }
        return new KafkaException("Failed to create diskless component from " + className, e);
    }

    private static final class IsolatedProvider implements DisklessStorageProvider {
        private final String className;
        private final URL[] urls;
        private final DisklessStorageProvider delegate;
        private final DisklessClassLoaderRegistry.Lease lease;
        private final AtomicBoolean closed = new AtomicBoolean();

        private IsolatedProvider(String className, URL[] urls, DisklessStorageProvider delegate,
                                 DisklessClassLoaderRegistry.Lease lease) {
            this.className = className;
            this.urls = urls;
            this.delegate = delegate;
            this.lease = lease;
        }

        @Override
        public DisklessEngine createBrokerEngine(DisklessEngineContext context) {
            return create(DisklessEngine.class, classLoader -> delegate.createBrokerEngine(
                context.withScheduler(DisklessClassLoaderContext.scheduler(context.scheduler(), classLoader))));
        }

        @Override
        public void configure(DisklessProviderContext context) throws Exception {
            DisklessClassLoaderContext.call(lease.classLoader(), () -> {
                delegate.configure(context);
                return null;
            });
        }

        @Override
        public DisklessTopicLifecycle createTopicLifecycle() {
            return create(DisklessTopicLifecycle.class, classLoader -> delegate.createTopicLifecycle());
        }

        private <T> T create(Class<T> type, ComponentFactory<T> factory) {
            if (closed.get()) {
                throw new IllegalStateException("Diskless storage provider " + className + " is closed");
            }
            DisklessClassLoaderRegistry.Lease componentLease = null;
            try {
                componentLease = DisklessClassLoaderRegistry.acquire(urls, DisklessEngine.class.getClassLoader());
                var classLoader = componentLease.classLoader();
                T component = DisklessClassLoaderContext.call(classLoader, () -> factory.create(classLoader));
                try {
                    return DisklessClassLoaderContext.leased(type, component, componentLease);
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
                if (componentLease != null) {
                    DisklessClassLoaderRegistry.closeLeaseOnFailure(componentLease, e);
                }
                throw propagate(className, e);
            }
        }

        @Override
        public void close() throws Exception {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            try {
                DisklessClassLoaderContext.call(lease.classLoader(), () -> {
                    delegate.close();
                    return null;
                });
            } finally {
                lease.close();
            }
        }
    }

    @FunctionalInterface
    private interface ComponentFactory<T> {
        T create(ClassLoader classLoader) throws Exception;
    }
}
