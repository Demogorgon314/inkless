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
package kafka.server;

import org.apache.kafka.storage.log.metrics.BrokerTopicMetrics;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import java.util.Objects;
import java.util.function.Consumer;

import io.aiven.inkless.engine.DisklessTopicMetrics;

/** Reports engine request outcomes to the broker topic metrics that classic topics also use. */
public final class DisklessBrokerTopicMetrics implements DisklessTopicMetrics {
    private final BrokerTopicStats stats;

    public DisklessBrokerTopicMetrics(BrokerTopicStats stats) {
        this.stats = Objects.requireNonNull(stats, "stats");
    }

    @Override
    public void markProduceRequest(String topic) {
        mark(topic, metrics -> metrics.totalProduceRequestRate().mark());
    }

    @Override
    public void markFailedProduceRequest(String topic) {
        mark(topic, metrics -> metrics.failedProduceRequestRate().mark());
    }

    @Override
    public void markBytesIn(String topic, long bytes, long messages) {
        mark(topic, metrics -> {
            metrics.bytesInRate(true).mark(bytes);
            metrics.messagesInRate().mark(messages);
        });
    }

    @Override
    public void markBytesRejected(String topic, long bytes) {
        mark(topic, metrics -> metrics.bytesRejectedRate().mark(bytes));
    }

    @Override
    public void markInvalidRecords(InvalidRecords kind) {
        var metrics = stats.allTopicsStats();
        switch (kind) {
            case MAGIC -> metrics.invalidMagicNumberRecordsPerSec().mark();
            case OFFSET_OR_SEQUENCE -> metrics.invalidOffsetOrSequenceRecordsPerSec().mark();
            case CHECKSUM -> metrics.invalidMessageCrcRecordsPerSec().mark();
            case NO_KEY_COMPACTED_TOPIC -> metrics.noKeyCompactedTopicRecordsPerSec().mark();
        }
    }

    @Override
    public void markFetchRequest(String topic) {
        mark(topic, metrics -> metrics.totalFetchRequestRate().mark());
    }

    @Override
    public void markFailedFetchRequest(String topic) {
        mark(topic, metrics -> metrics.failedFetchRequestRate().mark());
    }

    private void mark(String topic, Consumer<BrokerTopicMetrics> update) {
        update.accept(stats.topicStats(topic));
        update.accept(stats.allTopicsStats());
    }
}
