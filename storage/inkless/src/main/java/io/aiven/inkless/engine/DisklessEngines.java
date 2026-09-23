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

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/** Loads experimental providers from the broker classpath; isolated loading is outside this PoC. */
public final class DisklessEngines {
    public static final String CLASS_NAME_CONFIG = "diskless.engine.class.name";
    public static final String CONFIG_PREFIX = "diskless.engine.config.";

    private DisklessEngines() {
    }

    public static DisklessEngine load(Map<String, ?> configs, Supplier<DisklessEngine> nativeEngine) {
        Object className = configs.get(CLASS_NAME_CONFIG);
        if (className == null) {
            return nativeEngine.get();
        }
        final DisklessEngine engine;
        try {
            engine = Utils.newInstance(className.toString(), DisklessEngine.class);
        } catch (ClassNotFoundException e) {
            throw new ConfigException(CLASS_NAME_CONFIG, className, "Engine class not found");
        }
        Map<String, Object> engineConfig = new HashMap<>();
        configs.forEach((key, value) -> {
            if (key.startsWith(CONFIG_PREFIX)) {
                engineConfig.put(key.substring(CONFIG_PREFIX.length()), value);
            }
        });
        try {
            engine.configure(engineConfig);
            return engine;
        } catch (RuntimeException e) {
            try {
                engine.close();
            } catch (IOException | RuntimeException closeFailure) {
                e.addSuppressed(closeFailure);
            }
            throw e;
        }
    }
}
