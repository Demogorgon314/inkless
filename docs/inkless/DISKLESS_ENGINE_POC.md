<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements. See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Run Ursa through the diskless engine SPI

The PoC connects Inkless's broker routing to the real Ursa storage implementation
copied from UFK. Kafka clients can create a diskless topic, produce idempotently,
consume, list offsets, grow partitions, and recover records after broker
restarts. The external-engine path does not initialize the native Inkless
control plane, object storage, or maintenance workers.

This remains an experimental integration, not a complete implementation of
KIP-1163. Transactions, managed replicas, consolidation, classic-to-diskless
migration, and DeleteRecords are not implemented for the external provider.
The broker rejects the first four feature combinations or requests through its
existing diskless checks and the external-engine configuration validation.
DeleteRecords fails through the engine capability check; it does
not truncate Ursa data. The existing share-fetch path is not validated here.

## Architecture

```text
KafkaApis / ReplicaManager
  +-- classic partitions: existing local-log path
  +-- diskless partitions: DisklessEngine
        +-- default: existing Inkless handlers
        +-- isolated Ursa provider
              +-- UFK reader/writer and producer-state implementation
              +-- Lakestream catalog and Ursa object-storage runtime
              +-- Oxia metadata and producer-state snapshots

Active controller
  +-- DisklessTopicLifecycleReconciler
        +-- provider's DisklessTopicLifecycle
              +-- ensure, grow, reconfigure, delete, and sweep

Broker metadata updates
  +-- fence deleted topic IDs and close cached partition handles
  +-- apply changed topic configuration to existing handles
```

The broker keeps one dispatch layer. The integration does not copy UFK's
`DisklessStorageReplicaManagerSupport` beside Inkless's routing. The default
engine creates and owns its request handlers, delete interceptor, retention
enforcer, file cleaner, topic purger, and shared state. Its optional
`TieredStorage` service owns the dedicated consolidation reader and external
metadata operations. Broker code never obtains a native handler, cache, or
control-plane handle.

`DisklessEngineFactory` assembles native or isolated provider resources at
broker startup. `ReplicaManager` receives the resulting engine and owns its
shutdown; the broker closes it if construction fails before ownership transfers.
The broker stops its scheduler and drains fetchers before closing the engine.
The engine cancels its scheduled tasks and closes handlers before shared state.

| Boundary | Contract |
| --- | --- |
| `append`, `fetch`, `OffsetJob` | Asynchronous record operations; Kafka retains protocol routing and response handling. |
| `supportsDeleteRecords`, `deleteRecords` | Reject unsupported deletion before touching a local-log leg; return per-partition results or an exceptional future mapped by the broker. |
| `probeFetch` | Optional, ordered readiness hints with errors, watermark, and estimated bytes; no WAL coordinates cross the boundary. A cache miss is not authoritative. |
| `start`, `close` | Engine owns maintenance tasks and resources; startup runs once, and native close is idempotent. |
| `TieredStorage` | Optional fetch, cross-tier offset, prune, initialize, and repair operations. Kafka owns local-log coordination and retries. |
| `DisklessTopicLifecycle` | Separate controller service for topic creation, expansion, configuration, deletion, and reconciliation. |

The native tiered-storage metadata calls retain their synchronous behavior.
Turning them into asynchronous operations requires changing the surrounding
Kafka coordination paths; this refactor does not make that claim. Ursa returns
no tiered-storage capability, so it need not emulate Inkless migration or
consolidation semantics.

`ReplicaManager` retains `InklessMetadataView` for topic routing, leader epochs,
and cross-tier offset decisions. These broker responsibilities apply regardless
of the selected storage engine.

`DisklessEngine.Context` supplies Kafka-owned services and metadata lookups.
The Ursa adapter translates append, fetch, and offset lookup and invokes the
copied UFK implementation. Producer state uses UFK's stable `no-zone` namespace.
The SPI does not yet carry a reliable client zone; a broker's rack must not
change a producer's identity after failover.

`OffsetJob` preserves batching and cancellation in the existing ListOffsets
router. The isolated loader also establishes the provider context classloader
when invoking returned offset jobs, tiered-storage capabilities, and lifecycle services.

External engines have no native batch-coordinate cache. `DelayedFetch` hands
waiting to the engine's asynchronous fetch implementation when that cache is
absent. The native engine retains its original readiness probe. Kafka still
combines classic and diskless results and applies its response handling.

## Build and configure

Build the plugin separately from the broker runtime:

```bash
./gradlew :storage:ursa:assemblePlugin :storage:ursa:verifyPluginLicenses
```

The output is `storage/ursa/build/plugin/`. Keep this directory separate from
Kafka's `libs/`. Configure every broker and controller with the same provider
and storage settings:

```properties
diskless.storage.system.enable=true
diskless.engine.class.name=org.apache.kafka.storage.diskless.UrsaDisklessEngine
diskless.engine.class.path=/absolute/path/to/storage/ursa/build/plugin/*

diskless.engine.config.ursa.catalog.oxia.service.url=oxia://localhost:6648/default
diskless.engine.config.ursa.oxia.service.url=oxia://localhost:6648/default
diskless.engine.config.ursa.storage.backend.type=S3
diskless.engine.config.ursa.storage.path=ursa/wal
diskless.engine.config.ursa.storage.s3.endpoint=http://localhost:9000
diskless.engine.config.ursa.storage.s3.bucket=kafka-ursa
diskless.engine.config.ursa.storage.s3.region=us-east-1
diskless.engine.config.ursa.storage.s3.access.key=your-access-key
diskless.engine.config.ursa.storage.s3.secret.key=your-secret-key
diskless.engine.config.ursa.storage.s3.path.style.access=true
```

Create the bucket before using it. Create Kafka topics with
`diskless.enable=true`, replication factor 1, and `cleanup.policy=delete`.
Unconfigured topics keep their classic behavior. Omitting the engine properties
selects the native Inkless implementation.

The loader strips `diskless.engine.config.` before calling the provider.
The properties are experimental and are read from the original broker
configuration; they are not registered as stable Kafka public configuration.
There is no separate engine request-timeout setting. Existing request
deadlines and the copied UFK storage operations govern completion.

The plugin is not added to Kafka's release tarball by this PoC. Its separate
assembly includes a dependency license inventory, notices, and referenced
license texts. `verifyPluginLicenses` compares that inventory with the actual
assembled jars.

## Runtime and lifecycle ownership

The provider follows the Ursa 1.0.0 BOM independently of Kafka's forced
dependency versions. Kafka, logging, Scala, the engine SPI, and Yammer metrics
remain shared APIs. Provider-private dependencies load only from the plugin
runtime, with platform classes supplied by the JVM.

This strict boundary matters for optional dependencies: falling back to a
broker-side OpenTelemetry implementation can mix incompatible class identities.
The end-to-end test exercises the isolated runtime and checks that Oxia's API
is absent from the broker test classpath.

Broker and controller instances share a classloader lease for the same runtime.
The registry releases its reference when the last instance closes. Jar handles
are reclaimed with the classloader instead of being closed eagerly: gRPC may
still load classes during asynchronous shutdown after its client returns from
`close()`.

The controller loads the provider without a broker context and obtains its
lifecycle service. The provider owns that service and closes it at controller
shutdown. Only the active controller reconciles committed metadata. The copied
reconciler retains bounded concurrency, retries, metadata revisions, and orphan
recovery; its sweep interval is ten minutes. Retriable backend failures are
logged and metered, rather than treated as Kafka metadata replay faults.

Broker opens remain create-if-absent, so requests can race with controller
reconciliation. Both use the same topic ID, partition count, and metadata
revision. Deletion fences the immutable topic ID before retiring local handles;
a same-name replacement gets a different storage identity.

## Source provenance and maintenance

The source baseline is UFK commit `706699788b`; the Inkless baseline is
`e9de1e2e9b`. The implementation and license headers are copied, not rewritten.

- `storage/ursa/src/main/java/org/apache/kafka/storage/diskless/` contains UFK's
  data/read path, producer state, and catalog lifecycle implementation.
- `UrsaDisklessEngine` is the new broker adapter. `UrsaStorageConfig` uses
  provider-local copies of UFK's configuration defaults instead of adding Ursa
  keys to Kafka's server config class.
- `UrsaStorageState` is final to satisfy this checkout's constructor-escape
  compiler check. Its storage algorithms are unchanged.
- The generic lifecycle contract, reconciler, and reconciler tests come from
  UFK. Their imports and topic-enable key are adapted to Inkless.
- The loader utilities come from UFK, with stricter private dependency isolation,
  JVM platform delegation, returned-service context handling, and asynchronous
  runtime-shutdown handling.
- UFK's native storage API types remain private to the plugin. The broker API
  stays in the existing Inkless module for this PoC; upstream work should put it
  in a Kafka-owned API module.

Keep these copies identifiable when updating from UFK. Do not introduce a
second broker routing layer or expose Lakestream/Oxia types through the engine
SPI.

## Verification

`UrsaEngineIntegrationTest` runs real Kafka brokers with Oxia and MinIO. It
checks mixed classic/diskless consumption, acknowledged object-storage writes,
earliest/latest/timestamp offsets, rolling broker restarts, continued idempotent
production, partition growth, catalog deletion, and same-name topic recreation.
It uses no mocked storage.

```bash
docker info
./gradlew :core:test --tests kafka.server.UrsaEngineIntegrationTest
```

The core test task assembles and checks the plugin before running. The existing
native handler, ReplicaManager, offset-router, KafkaApis, configuration,
metadata-publisher, and delayed-fetch tests cover the affected broker behavior.
The copied lifecycle tests cover retry, ordering, leadership loss, and sweeps.
Consolidation and migration tests cover the native tiered-storage adapter.
Engine tests cover task cancellation and resource closure after a handler fails;
broker tests cover exceptional plugin deletion results.
This is targeted validation, not the full Kafka test suite or a performance
benchmark.

## Implications for the KIP proposal

A record-level SPI can preserve the native implementation while allowing Ursa
to own its format, ordering, and producer state. Keep `ObjectStorage` inside
the default engine. The integration also needs the lifecycle and asynchronous
fetch boundaries demonstrated here.

[KIP-1163](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/350783976/KIP-1163+Diskless+Core)
and [KIP-1164](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/350783984/KIP-1164+Diskless+Coordinator)
still require a broader agreement on replicas, transactions, and compatibility.
This PoC does not establish those semantics for external engines. The public
SPI should also use topic-ID-aware offset requests/results instead of exposing
Kafka's private purgatory job shape.
