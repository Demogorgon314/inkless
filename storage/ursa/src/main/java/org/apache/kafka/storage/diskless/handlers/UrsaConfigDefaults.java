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
package org.apache.kafka.storage.diskless.handlers;

/** Configuration defaults copied from UFK's ServerLogConfigs for the isolated provider. */
final class UrsaConfigDefaults {
    private UrsaConfigDefaults() {
    }

    static final String URSA_STORAGE_ENABLE_CONFIG = "ursa.storage.enable";
    static final boolean URSA_STORAGE_ENABLE_DEFAULT = false;
    static final String URSA_CATALOG_OXIA_SERVICE_URL_CONFIG = "ursa.catalog.oxia.service.url";
    static final String URSA_CATALOG_OXIA_SERVICE_URL_DEFAULT = "oxia://localhost:6648/default";
    static final String URSA_OXIA_SERVICE_URL_CONFIG = "ursa.oxia.service.url";
    static final String URSA_OXIA_SERVICE_URL_DEFAULT = "oxia://localhost:6648/default";
    static final String URSA_STORAGE_BACKEND_TYPE_CONFIG = "ursa.storage.backend.type";
    static final String URSA_STORAGE_BACKEND_TYPE_DEFAULT = "LOCAL";
    static final String URSA_STORAGE_PATH_CONFIG = "ursa.storage.path";
    static final String URSA_STORAGE_PATH_DEFAULT = "/tmp/ursa-data";
    static final String URSA_STORAGE_COMPACTION_PREFIX_CONFIG = "ursa.storage.compaction.prefix";
    static final String URSA_STORAGE_COMPACTION_PREFIX_DEFAULT = "/tmp/compaction-data";
    static final String URSA_STORAGE_WRITE_BUFFER_FLUSH_INTERVAL_MS_CONFIG = "ursa.storage.write.buffer.flush.interval.ms";
    static final long URSA_STORAGE_WRITE_BUFFER_FLUSH_INTERVAL_MS_DEFAULT = 250L;
    static final String URSA_STORAGE_WRITE_BUFFER_SIZE_CONFIG = "ursa.storage.write.buffer.size";
    static final int URSA_STORAGE_WRITE_BUFFER_SIZE_DEFAULT = 4 * 1024 * 1024;
    static final String URSA_STORAGE_WRITE_BUFFER_FLUSH_SIZE_CONFIG = "ursa.storage.write.buffer.flush.size";
    static final long URSA_STORAGE_WRITE_BUFFER_FLUSH_SIZE_DEFAULT = 256 * 1024 * 1024L;
    static final String URSA_STORAGE_S3_ENDPOINT_CONFIG = "ursa.storage.s3.endpoint";
    static final String URSA_STORAGE_S3_ENDPOINT_DEFAULT = "";
    static final String URSA_STORAGE_S3_ACCESS_KEY_CONFIG = "ursa.storage.s3.access.key";
    static final String URSA_STORAGE_S3_ACCESS_KEY_DEFAULT = "";
    static final String URSA_STORAGE_S3_SECRET_KEY_CONFIG = "ursa.storage.s3.secret.key";
    static final String URSA_STORAGE_S3_SECRET_KEY_DEFAULT = "";
    static final String URSA_STORAGE_S3_SESSION_TOKEN_CONFIG = "ursa.storage.s3.session.token";
    static final String URSA_STORAGE_S3_PATH_STYLE_ACCESS_CONFIG = "ursa.storage.s3.path.style.access";
    static final String URSA_STORAGE_S3_BUCKET_CONFIG = "ursa.storage.s3.bucket";
    static final String URSA_STORAGE_S3_BUCKET_DEFAULT = "kafka-ursa-storage";
    static final String URSA_STORAGE_COMPACTION_BUCKET_CONFIG = "ursa.storage.compaction.bucket";
    static final String URSA_STORAGE_S3_REGION_CONFIG = "ursa.storage.s3.region";
    static final String URSA_STORAGE_S3_REGION_DEFAULT = "us-east-1";
    static final String URSA_STORAGE_PRODUCER_STATE_SNAPSHOT_INTERVAL_MS_CONFIG = "ursa.storage.producer.state.snapshot.interval.ms";
    static final long URSA_STORAGE_PRODUCER_STATE_SNAPSHOT_INTERVAL_MS_DEFAULT = 30_000L;
    static final String URSA_STORAGE_PRODUCER_STATE_SNAPSHOT_RECORD_THRESHOLD_CONFIG = "ursa.storage.producer.state.snapshot.record.threshold";
    static final int URSA_STORAGE_PRODUCER_STATE_SNAPSHOT_RECORD_THRESHOLD_DEFAULT = 10_000;
    static final String URSA_STORAGE_CLASS_PATH_CONFIG = "ursa.storage.class.path";
}
