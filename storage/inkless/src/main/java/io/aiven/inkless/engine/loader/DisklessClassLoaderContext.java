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

import org.apache.kafka.server.util.Scheduler;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import io.aiven.inkless.engine.DisklessEngine;

final class DisklessClassLoaderContext {
    private static final String SPI_PACKAGE = DisklessEngine.class.getPackageName();

    private DisklessClassLoaderContext() {
    }

    static <T> T call(ClassLoader classLoader, Action<T> action) throws Exception {
        ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(classLoader);
        try {
            return action.execute();
        } finally {
            Thread.currentThread().setContextClassLoader(originalClassLoader);
        }
    }

    /**
     * Wraps one isolated-runtime component so every call runs with the plugin context class loader
     * and the first {@code close()} releases the class-loader lease.
     *
     * <p>The proxy exposes every SPI interface the delegate implements, so callers can distinguish
     * lifecycle contracts without seeing the plugin class. Extensions returned by the delegate get
     * the same class-loader context but do not own the lease.
     *
     * <p>The lease is released even when the delegate fails to close: a component that cannot close
     * cleanly must not pin its runtime for the lifetime of the process. Subsequent {@code close()}
     * calls are no-ops.
     */
    static <T> T leased(Class<T> type, T delegate, DisklessClassLoaderRegistry.Lease lease) {
        Objects.requireNonNull(delegate, "delegate must not be null");
        Objects.requireNonNull(lease, "lease must not be null");
        if (!type.isInstance(delegate)) {
            throw new IllegalArgumentException(delegate.getClass().getName() + " does not implement " + type.getName());
        }
        return type.cast(proxy(delegate, new ContextHandler(delegate, lease.classLoader(), lease)));
    }

    /** Returns a scheduler whose tasks run with the plugin context class loader. */
    static Scheduler scheduler(Scheduler delegate, ClassLoader classLoader) {
        return new ContextScheduler(delegate, classLoader);
    }

    private static Object proxy(Object delegate, InvocationHandler handler) {
        Set<Class<?>> interfaces = new LinkedHashSet<>();
        for (Class<?> type = delegate.getClass(); type != null; type = type.getSuperclass()) {
            collectSpiInterfaces(type, interfaces);
        }
        return Proxy.newProxyInstance(DisklessEngine.class.getClassLoader(), interfaces.toArray(Class<?>[]::new), handler);
    }

    private static void collectSpiInterfaces(Class<?> type, Set<Class<?>> interfaces) {
        for (Class<?> candidate : type.getInterfaces()) {
            if (candidate.getPackageName().equals(SPI_PACKAGE)) {
                interfaces.add(candidate);
            }
            collectSpiInterfaces(candidate, interfaces);
        }
    }

    private static Object invoke(Method method, Object target, Object[] args) throws Exception {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception exception) {
                throw exception;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw e;
        }
    }

    /**
     * A named handler rather than a lambda: it keeps the isolated delegate discoverable in stack
     * traces and from tests that reflect into the plugin runtime.
     */
    private static final class ContextHandler implements InvocationHandler {
        private final Object delegate;
        private final ClassLoader classLoader;
        private final DisklessClassLoaderRegistry.Lease lease;
        private final AtomicBoolean closed = new AtomicBoolean();

        private ContextHandler(Object delegate, ClassLoader classLoader, DisklessClassLoaderRegistry.Lease lease) {
            this.delegate = delegate;
            this.classLoader = classLoader;
            this.lease = lease;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (method.getDeclaringClass() == Object.class) {
                // The proxy, not the delegate, is the component every caller holds, so it keeps its
                // own identity: forwarding equals/hashCode would make proxy.equals(proxy) false.
                // toString still forwards, so logs and stack traces name the isolated delegate.
                if ("equals".equals(method.getName()) && method.getParameterCount() == 1) {
                    return proxy == args[0];
                }
                if ("hashCode".equals(method.getName()) && method.getParameterCount() == 0) {
                    return System.identityHashCode(proxy);
                }
                return method.invoke(delegate, args);
            }
            if (lease != null && "close".equals(method.getName()) && method.getParameterCount() == 0) {
                if (!closed.compareAndSet(false, true)) {
                    return null;
                }
                try {
                    return call(classLoader, () -> DisklessClassLoaderContext.invoke(method, delegate, args));
                } finally {
                    lease.close();
                }
            }
            Object result = call(classLoader, () -> DisklessClassLoaderContext.invoke(method, delegate, args));
            if (result instanceof Optional<?> extension && extension.isPresent()) {
                return Optional.of(proxy(extension.get(), new ContextHandler(extension.get(), classLoader, null)));
            }
            return result;
        }
    }

    /** Lets a plugin schedule tasks without controlling the lifecycle of Kafka's scheduler. */
    private record ContextScheduler(Scheduler delegate, ClassLoader classLoader) implements Scheduler {
        @Override
        public void startup() {
            throw new UnsupportedOperationException("Kafka owns the scheduler lifecycle");
        }

        @Override
        public void shutdown() {
            throw new UnsupportedOperationException("Kafka owns the scheduler lifecycle");
        }

        @Override
        public ScheduledFuture<?> schedule(String name, Runnable task, long delayMs, long periodMs) {
            return delegate.schedule(name, () -> {
                try {
                    call(classLoader, () -> {
                        task.run();
                        return null;
                    });
                } catch (RuntimeException | Error e) {
                    throw e;
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }, delayMs, periodMs);
        }

        @Override
        public void resizeThreadPool(int newSize) {
            throw new UnsupportedOperationException("Kafka owns the scheduler lifecycle");
        }
    }

    interface Action<T> {
        T execute() throws Exception;
    }
}
