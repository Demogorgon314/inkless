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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import io.aiven.inkless.engine.DisklessEngines;
import io.aiven.inkless.engine.DisklessTopicLifecycle;
import io.aiven.inkless.test_utils.MinioContainer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 240, unit = TimeUnit.SECONDS)
public class UrsaEngineIntegrationTest {
    private static final String DISKLESS = "ursa-records";
    private static final String CLASSIC = "classic-records";

    @Test
    public void persistsRecordsAcrossBrokerRestartsWithoutNativeControlPlane() throws Exception {
        assertThrows(ClassNotFoundException.class, () -> Class.forName("io.oxia.client.api.OxiaClientBuilder"));
        try (var oxia = new GenericContainer<>(DockerImageName.parse("oxia/oxia:0.17.1"))
                 .withExposedPorts(6648)
                 .withCommand("/oxia/bin/oxia", "standalone", "--data-dir=/data", "--shards=8");
             var minio = new MinioContainer(DockerImageName.parse("minio/minio:latest"))) {
            oxia.start();
            minio.start();
            minio.createBucket("ursa-engine-test");
            String oxiaUrl = "oxia://" + oxia.getHost() + ":" + oxia.getMappedPort(6648) + "/default";
            try (var cluster = new KafkaClusterTestKit.Builder(new TestKitNodes.Builder()
                    .setNumBrokerNodes(2).setNumControllerNodes(1).build())
                    .setConfigProp(ServerConfigs.DISKLESS_STORAGE_SYSTEM_ENABLE_CONFIG, "true")
                    .setConfigProp(DisklessEngines.CLASS_NAME_CONFIG,
                        "org.apache.kafka.storage.diskless.UrsaStorageProvider")
                    .setConfigProp(DisklessEngines.CLASS_PATH_CONFIG, System.getProperty("ursa.engine.class.path"))
                    .setConfigProp(DisklessEngines.CONFIG_PREFIX + "ursa.catalog.oxia.service.url", oxiaUrl)
                    .setConfigProp(DisklessEngines.CONFIG_PREFIX + "ursa.oxia.service.url", oxiaUrl)
                    .setConfigProp(DisklessEngines.CONFIG_PREFIX + "ursa.storage.backend.type", "S3")
                    .setConfigProp(DisklessEngines.CONFIG_PREFIX + "ursa.storage.s3.endpoint", minio.getEndpoint())
                    .setConfigProp(DisklessEngines.CONFIG_PREFIX + "ursa.storage.s3.access.key", minio.getAccessKey())
                    .setConfigProp(DisklessEngines.CONFIG_PREFIX + "ursa.storage.s3.secret.key", minio.getSecretKey())
                    .setConfigProp(DisklessEngines.CONFIG_PREFIX + "ursa.storage.s3.bucket", minio.getBucketName())
                    .setConfigProp(DisklessEngines.CONFIG_PREFIX + "ursa.storage.s3.path.style.access", "true")
                    .setConfigProp(DisklessEngines.CONFIG_PREFIX + "ursa.storage.path", "engine-test")
                    .setConfigProp(DisklessEngines.CONFIG_PREFIX + "ursa.storage.write.buffer.flush.interval.ms", "10")
                    .setConfigProp(DisklessEngines.CONFIG_PREFIX + "ursa.storage.producer.state.snapshot.record.threshold", "1")
                    .setConfigProp("offsets.topic.replication.factor", "1")
                    .build()) {
                cluster.format();
                cluster.startup();
                cluster.waitForReadyBrokers();
                cluster.brokers().values().forEach(broker ->
                    assertTrue(broker.sharedServer().inklessControlPlane().isEmpty()));

                Map<String, Object> clientConfig = new HashMap<>();
                clientConfig.put("bootstrap.servers", cluster.bootstrapServers());
                clientConfig.put("request.timeout.ms", "10000");
                clientConfig.put("delivery.timeout.ms", "15000");
                try (Admin admin = Admin.create(clientConfig);
                     var producer = new KafkaProducer<>(clientConfig, new StringSerializer(), new StringSerializer())) {
                    admin.createTopics(List.of(
                        new NewTopic(DISKLESS, 2, (short) 1).configs(Map.of(TopicConfig.DISKLESS_ENABLE_CONFIG, "true")),
                        new NewTopic(CLASSIC, 1, (short) 1))).all().get(30, TimeUnit.SECONDS);
                    for (int i = 0; i < 20; i++) {
                        for (int p = 0; p < 2; p++) {
                            assertEquals(i, producer.send(new ProducerRecord<>(DISKLESS, p, 1000L + i,
                                "key-" + i, "value-" + i)).get(30, TimeUnit.SECONDS).offset());
                        }
                        producer.send(new ProducerRecord<>(CLASSIC, "classic-" + i)).get(30, TimeUnit.SECONDS);
                    }
                    verifyRead(clientConfig, 20);
                    var partition = new TopicPartition(DISKLESS, 0);
                    assertEquals(20L, admin.listOffsets(Map.of(partition, OffsetSpec.latest()))
                        .all().get(30, TimeUnit.SECONDS).get(partition).offset());
                    assertEquals(0L, admin.listOffsets(Map.of(partition, OffsetSpec.earliest()))
                        .all().get(30, TimeUnit.SECONDS).get(partition).offset());
                    assertEquals(10L, admin.listOffsets(Map.of(partition, OffsetSpec.forTimestamp(1010L)))
                        .all().get(30, TimeUnit.SECONDS).get(partition).offset());
                    try (var s3 = minio.getS3Client()) {
                        assertTrue(s3.listObjectsV2(request -> request.bucket(minio.getBucketName()))
                            .keyCount() > 0, "Acknowledged data must reach object storage");
                    }

                    for (var broker : cluster.brokers().values()) {
                        broker.shutdown();
                        broker.awaitShutdown();
                        broker.startup();
                        cluster.waitForReadyBrokers();
                    }
                    cluster.waitForReadyBrokers();
                    clientConfig.put("bootstrap.servers", cluster.bootstrapServers());
                    verifyRead(clientConfig, 20);
                    // Keep the producer ID and sequence across the restart to exercise recovered state.
                    for (int p = 0; p < 2; p++) {
                        assertEquals(20L, producer.send(new ProducerRecord<>(DISKLESS, p, 1020L,
                            "key-20", "value-20")).get(30, TimeUnit.SECONDS).offset());
                    }
                    verifyRead(clientConfig, 21);
                    admin.createPartitions(Map.of(DISKLESS, NewPartitions.increaseTo(3)))
                        .all().get(30, TimeUnit.SECONDS);
                    assertEquals(0L, producer.send(new ProducerRecord<>(DISKLESS, 2, null, "new-partition"))
                        .get(30, TimeUnit.SECONDS).offset());

                    var oldId = admin.describeTopics(List.of(DISKLESS)).allTopicNames()
                        .get(30, TimeUnit.SECONDS).get(DISKLESS).topicId();
                    var providerConfig = cluster.controllers().values().iterator().next().config().originals();
                    try (var inspection = DisklessEngines.loadLifecycle(providerConfig)) {
                        var lifecycle = (DisklessTopicLifecycle.MetadataDriven) inspection;
                        TestUtils.waitForCondition(() -> lifecycle.listManagedTopics().get(10, TimeUnit.SECONDS)
                            .stream().anyMatch(topic -> topic.topicId().equals(oldId)),
                            30000, "Controller must reconcile the Ursa catalog");
                        admin.deleteTopics(List.of(DISKLESS)).all().get(30, TimeUnit.SECONDS);
                        TestUtils.waitForCondition(() -> lifecycle.listManagedTopics().get(10, TimeUnit.SECONDS)
                            .stream().noneMatch(topic -> topic.topicId().equals(oldId)),
                            30000, "Controller must delete the old topic incarnation from Ursa");
                        admin.createTopics(List.of(new NewTopic(DISKLESS, 1, (short) 1)
                            .configs(Map.of(TopicConfig.DISKLESS_ENABLE_CONFIG, "true"))))
                            .all().get(30, TimeUnit.SECONDS);
                        try (var recreatedProducer = new KafkaProducer<>(clientConfig,
                                new StringSerializer(), new StringSerializer())) {
                            assertEquals(0L, recreatedProducer.send(new ProducerRecord<>(DISKLESS, "recreated"))
                                .get(30, TimeUnit.SECONDS).offset());
                        }
                    }
                }
            }
        }
    }

    private static void verifyRead(Map<String, Object> clientConfig, int expectedPerPartition) {
        Map<String, Object> config = new HashMap<>(clientConfig);
        config.put("enable.auto.commit", "false");
        try (var consumer = new KafkaConsumer<>(config, new StringDeserializer(), new StringDeserializer())) {
            var partitions = List.of(new TopicPartition(DISKLESS, 0), new TopicPartition(DISKLESS, 1),
                new TopicPartition(CLASSIC, 0));
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            Map<TopicPartition, Integer> counts = new HashMap<>();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
            while (counts.values().stream().mapToInt(Integer::intValue).sum() < expectedPerPartition * 2 + 20
                    && System.nanoTime() < deadline) {
                for (var record : consumer.poll(Duration.ofMillis(100))) {
                    var partition = new TopicPartition(record.topic(), record.partition());
                    int next = counts.getOrDefault(partition, 0);
                    assertEquals(next, record.offset());
                    assertEquals(record.topic().equals(CLASSIC) ? "classic-" + next : "value-" + next, record.value());
                    counts.put(partition, next + 1);
                }
            }
            partitions.forEach(partition -> assertEquals(partition.topic().equals(CLASSIC) ? 20 : expectedPerPartition,
                counts.getOrDefault(partition, 0)));
        }
    }
}
