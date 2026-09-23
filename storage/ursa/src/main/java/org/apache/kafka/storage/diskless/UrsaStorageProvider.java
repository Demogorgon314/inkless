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

import org.apache.kafka.storage.diskless.handlers.UrsaDisklessTopicLifecycle;
import org.apache.kafka.storage.diskless.handlers.UrsaStorageConfig;

import java.util.Map;

import io.aiven.inkless.engine.DisklessEngine;
import io.aiven.inkless.engine.DisklessStorageProvider;
import io.aiven.inkless.engine.DisklessTopicLifecycle;

/** Creates independent broker and controller resources from the same plugin configuration. */
public final class UrsaStorageProvider implements DisklessStorageProvider {
    @Override
    public DisklessEngine createBrokerEngine(Map<String, ?> configs, DisklessEngine.Context context) throws Exception {
        return new UrsaDisklessEngine(UrsaStorageConfig.fromConfigs(configs), context);
    }

    @Override
    public DisklessTopicLifecycle createTopicLifecycle(Map<String, ?> configs) throws Exception {
        return new UrsaDisklessTopicLifecycle(UrsaStorageConfig.fromConfigs(configs));
    }
}
