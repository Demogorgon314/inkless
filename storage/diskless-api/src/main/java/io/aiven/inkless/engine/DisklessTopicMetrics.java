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

/**
 * Records diskless request outcomes in the broker topic metrics that classic topics also report.
 * Each topic-level call also updates the all-topics aggregate. Calls are cheap and thread-safe.
 */
public interface DisklessTopicMetrics {
    /** Kinds of invalid records that record validation rejects. */
    enum InvalidRecords { MAGIC, OFFSET_OR_SEQUENCE, CHECKSUM, NO_KEY_COMPACTED_TOPIC }

    void markProduceRequest(String topic);

    void markFailedProduceRequest(String topic);

    void markBytesIn(String topic, long bytes, long messages);

    void markBytesRejected(String topic, long bytes);

    /** Updates only the all-topics aggregate, as record validation does for classic topics. */
    void markInvalidRecords(InvalidRecords kind);

    void markFetchRequest(String topic);

    void markFailedFetchRequest(String topic);

    /** Returns metrics that discard every update. */
    static DisklessTopicMetrics noop() {
        return NoopDisklessTopicMetrics.INSTANCE;
    }
}

final class NoopDisklessTopicMetrics implements DisklessTopicMetrics {
    static final NoopDisklessTopicMetrics INSTANCE = new NoopDisklessTopicMetrics();

    private NoopDisklessTopicMetrics() {
    }

    @Override
    public void markProduceRequest(String topic) {
    }

    @Override
    public void markFailedProduceRequest(String topic) {
    }

    @Override
    public void markBytesIn(String topic, long bytes, long messages) {
    }

    @Override
    public void markBytesRejected(String topic, long bytes) {
    }

    @Override
    public void markInvalidRecords(InvalidRecords kind) {
    }

    @Override
    public void markFetchRequest(String topic) {
    }

    @Override
    public void markFailedFetchRequest(String topic) {
    }
}
