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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.ChildFirstClassLoader;
import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/** Loads engines with shared Kafka APIs and an isolated provider dependency runtime. */
public final class DisklessEngines {
    public static final String CLASS_NAME_CONFIG = "diskless.engine.class.name";
    public static final String CLASS_PATH_CONFIG = "diskless.engine.class.path";
    public static final String CONFIG_PREFIX = "diskless.engine.config.";

    private DisklessEngines() {
    }

    public static DisklessEngine load(Map<String, ?> configs, Supplier<DisklessEngine> nativeEngine) {
        return load(configs, nativeEngine, null);
    }

    public static DisklessEngine load(Map<String, ?> configs, Supplier<DisklessEngine> nativeEngine,
                                      DisklessEngine.Context context) {
        Object className = configs.get(CLASS_NAME_CONFIG);
        if (className == null) {
            return nativeEngine.get();
        }
        DisklessClassLoaderRegistry.Lease lease = null;
        try {
            Object classPath = configs.get(CLASS_PATH_CONFIG);
            URL[] urls = new URL[0];
            if (classPath != null) {
                try (var paths = new ChildFirstClassLoader(classPath.toString(), DisklessEngine.class.getClassLoader())) {
                    urls = paths.getURLs();
                }
                if (urls.length == 0) {
                    throw new ConfigException(CLASS_PATH_CONFIG, classPath, "No plugin files found");
                }
            }
            lease = DisklessClassLoaderRegistry.acquire(urls, DisklessEngine.class.getClassLoader());
            var acquired = lease;
            DisklessEngine engine = DisklessClassLoaderContext.call(lease.classLoader(), () -> {
                DisklessEngine instance = Utils.newInstance(
                    Class.forName(className.toString(), true, acquired.classLoader()).asSubclass(DisklessEngine.class));
                configure(instance, configs, context);
                return instance;
            });
            return DisklessClassLoaderContext.leased(DisklessEngine.class, engine, lease);
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
            throw new KafkaException("Failed to initialize diskless engine " + className, e);
        }
    }

    private static void configure(DisklessEngine engine, Map<String, ?> configs, DisklessEngine.Context context) {
        Map<String, Object> engineConfig = new HashMap<>();
        configs.forEach((key, value) -> {
            if (key.startsWith(CONFIG_PREFIX)) {
                engineConfig.put(key.substring(CONFIG_PREFIX.length()), value);
            }
        });
        try {
            engine.configure(engineConfig);
            if (context != null) {
                engine.initialize(context);
            }
        } catch (RuntimeException | Error e) {
            try {
                engine.close();
            } catch (IOException | RuntimeException | Error closeFailure) {
                e.addSuppressed(closeFailure);
            }
            throw e;
        }
    }
}
