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
package org.apache.kafka.storage.diskless;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.diskless.handlers.UrsaDisklessTopicLifecycle;
import org.apache.kafka.storage.diskless.handlers.UrsaStorageConfig;

import java.util.HashMap;
import java.util.Map;

import io.aiven.inkless.engine.DisklessEngine;
import io.aiven.inkless.engine.DisklessEngineContext;
import io.aiven.inkless.engine.DisklessProviderContext;
import io.aiven.inkless.engine.DisklessStorageProvider;
import io.aiven.inkless.engine.DisklessTopicLifecycle;

/** Creates independent broker and controller resources from the same plugin configuration. */
public final class UrsaStorageProvider implements DisklessStorageProvider {
    /** Namespace of the Ursa settings in the broker configuration. */
    public static final String CONFIG_PREFIX = "diskless.engine.config.";

    private UrsaStorageConfig config;
    private Time time;

    @Override
    public void configure(DisklessProviderContext context) throws Exception {
        config = UrsaStorageConfig.fromConfigs(withoutPrefix(context.configs()));
        time = context.time();
    }

    @Override
    public DisklessEngine createBrokerEngine(DisklessEngineContext context) {
        return new UrsaDisklessEngine(configured(), time, context);
    }

    @Override
    public DisklessTopicLifecycle createTopicLifecycle() throws Exception {
        return new UrsaDisklessTopicLifecycle(configured());
    }

    private UrsaStorageConfig configured() {
        if (config == null) {
            throw new IllegalStateException("Provider is not configured");
        }
        return config;
    }

    static Map<String, Object> withoutPrefix(Map<String, ?> brokerConfigs) {
        Map<String, Object> properties = new HashMap<>();
        brokerConfigs.forEach((key, value) -> {
            if (key.startsWith(CONFIG_PREFIX)) {
                properties.put(key.substring(CONFIG_PREFIX.length()), value);
            }
        });
        return Map.copyOf(properties);
    }
}
