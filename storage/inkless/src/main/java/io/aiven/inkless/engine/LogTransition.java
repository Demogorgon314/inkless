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
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Errors;

import java.util.List;

/**
 * Takes over classic logs after Kafka commits the transition.
 * Kafka owns the transition state machine and retries; implementations own external log metadata.
 * Methods retain the synchronous contract and require the LOG_TRANSITION capability.
 */
public interface LogTransition {
    record ProducerState(long producerId, short producerEpoch, int baseSequence, int lastSequence,
                         long assignedOffset, long batchMaxTimestamp) { }

    record LogInitialization(Uuid topicId, String topicName, int partition,
                             long logStartOffset, long disklessStartOffset, List<ProducerState> producerStates) { }

    /**
     * Applies the seal and producer state after KRaft commits the transition, on the leader only.
     * Returns outcomes in request order; INVALID_REQUEST means already initialized.
     */
    default List<Errors> initializeLogs(List<LogInitialization> requests) {
        throw new UnsupportedOperationException("Log transition is not supported");
    }

    /** Reconciles existing external log metadata with the seal committed in KRaft. */
    default Errors repairLog(TopicIdPartition partition, long startOffset) {
        throw new UnsupportedOperationException("Log transition is not supported");
    }
}
