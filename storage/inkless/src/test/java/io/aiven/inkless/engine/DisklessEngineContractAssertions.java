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
import org.apache.kafka.common.requests.ListOffsetsRequest;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Shared assertions run against native Inkless and the isolated Ursa runtime. */
public final class DisklessEngineContractAssertions {
    private DisklessEngineContractAssertions() {
    }

    public static void assertOffsetBatch(DisklessEngine engine, TopicIdPartition known,
                                          TopicIdPartition missing, long expectedLatest) throws Exception {
        var spec = new OffsetReader.ListOffsetsSpec(ListOffsetsRequest.LATEST_TIMESTAMP, Optional.empty());
        var requests = Map.of(known, spec, missing, spec);
        var response = engine.listOffsets(requests).get(10, TimeUnit.SECONDS);
        assertEquals(requests.keySet(), response.keySet(), "A failed partition must not disappear from the batch");
        assertEquals(Errors.NONE, response.get(known).error());
        assertEquals(expectedLatest, response.get(known).offset());
        assertNotEquals(Errors.NONE, response.get(missing).error());
        assertTrue(engine.listOffsets(Map.of()).get(10, TimeUnit.SECONDS).isEmpty());
    }
}
