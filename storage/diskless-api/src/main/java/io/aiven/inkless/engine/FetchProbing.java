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

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.protocol.Errors;

import java.util.List;

/**
 * Reports whether fetches can complete, so Kafka can hold them in its purgatory.
 *
 * <p>Without this extension, Kafka completes a delayed diskless fetch immediately and the engine's
 * fetch implementation owns any waiting.
 */
@FunctionalInterface
public interface FetchProbing {
    record FetchProbe(TopicIdPartition partition, long offset, int maxBytes) { }

    /** hasData distinguishes a known batch range from a local cache miss. */
    record FetchAvailability(TopicIdPartition partition, Errors error, boolean hasData,
                             long highWatermark, long estimatedBytes) { }

    /**
     * Returns one hint per request, in request order. Runs inside purgatory completion checks.
     * A cache miss is not authoritative and cannot replace a fetch.
     */
    List<FetchAvailability> probeFetch(List<FetchProbe> requests);
}
