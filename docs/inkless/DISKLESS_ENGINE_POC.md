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
  +-- DisklessTopicLifecycle
        +-- request-driven: ControllerApis -> InklessTopicLifecycle
        +-- metadata-driven: Reconciler -> UrsaDisklessTopicLifecycle

Broker metadata updates
  +-- fence deleted topic IDs and close cached partition handles
  +-- apply changed topic configuration to existing handles
```

The broker keeps one dispatch layer. The integration does not copy UFK's
`DisklessStorageReplicaManagerSupport` beside Inkless's routing. The default
engine creates and owns its request handlers, delete interceptor, retention
enforcer, file cleaner, topic purger, and shared state. Its internal
`InklessConsolidation` service owns the dedicated consolidation reader and
cross-tier retention coordination. This service is not part of the provider SPI;
external engines neither expose it nor implement an empty substitute.
`LogTransitionSupport` handles classic-log initialization and repair independently.
Broker code never obtains a native handler, cache, or control-plane handle.

`DisklessEngineFactory` assembles native or isolated provider resources at
broker startup. For native Inkless, the factory also supplies a borrowed
`InklessConsolidation` service through a separate broker constructor parameter.
External providers supply only their engine. `ReplicaManager` owns engine
shutdown; the broker closes it if construction fails before ownership transfers.
The broker stops its scheduler and drains fetchers before closing the engine.
The engine cancels its scheduled tasks and closes handlers before shared state.

| Boundary | Contract |
| --- | --- |
| `Appender`, `Fetcher`, `OffsetReader` | Asynchronous record operations; Kafka retains protocol routing and response handling. |
| `recordDeleter` | Reject unsupported deletion before touching a local-log leg; return per-partition results or an exceptional future mapped by the broker. |
| `fetchProber` | Optional, ordered readiness hints with errors, watermark, and estimated bytes; no WAL coordinates cross the boundary. A cache miss is not authoritative. |
| `start`, `close` | Engine owns maintenance tasks and resources; startup runs once, and native close is idempotent. |
| `LogTransitionSupport` | Optional initialization and repair after Kafka commits a classic-to-diskless transition. Kafka owns coordination and retries. |
| `DisklessTopicLifecycle` | Separate controller service for topic creation, expansion, configuration, deletion, and reconciliation. |

`DisklessEngine` extends `Appender`, `Fetcher`, and `OffsetReader`. Both providers
implement one engine and share its resources across those operations. Callers
that only need reads or offset lookups depend on the smaller interface; there
are no independently configured reader or writer plugins.

The native consolidation and transition metadata calls retain their synchronous behavior.
Turning them into asynchronous operations requires changing the surrounding
Kafka coordination paths; this refactor does not make that claim. Ursa does not
provide log-transition support. Inkless consolidation is assembled independently
of the public SPI; Ursa does not participate in that implementation-specific flow.

`ReplicaManager` retains `InklessMetadataView` for topic routing, leader epochs,
and cross-tier offset decisions. These broker responsibilities apply regardless
of the selected storage engine.

`DisklessEngine.Context` supplies Kafka-owned services, broker identity, and a
supplier of immutable metadata snapshots. Each snapshot resolves diskless topics
by UUID and reads partition count, raw topic overrides, and source revision from
one Kafka metadata image. Each snapshot caches resolved topics by UUID. Broker
defaults remain separate. Configuration-change callbacks receive the same
`TopicMetadata` representation, including raw overrides and source revision,
from the image being published; they do not read the broker's name-based config
cache. Removing an override therefore leaves the engine to apply its broker default. Ursa uses one topic
snapshot to open a partition and initialize its writer; retention and handle
reconciliation also resolve by UUID. A same-name replacement cannot supply the
old partition's configuration. A snapshot does not fence subsequent metadata
changes; the existing storage deletion fence still protects deleted identities.

The Ursa adapter translates append, fetch, and offset lookup and invokes the
copied UFK implementation. Append receives an immutable `DisklessRequestContext`
with client ID and listener. Producer state retains UFK's stable
`no-zone` namespace: client routing hints alone do not establish zone ownership.
Adding zone-based producer identity requires coordinated routing and owner reconciliation.

Kafka's `DisklessOffsetJob` owns batching, cancellation, and purgatory conversion.
It resolves immutable topic IDs before dispatching a batch to `listOffsets`.
The engine returns typed results without Kafka's private `FileRecordsOrError`.
Cancellation stops waiting and attempts to cancel backend work; it does not promise
that the backend has stopped. Normal batch completion includes every requested
partition, including failures; an exceptional append does not prove that nothing
was committed. Engines own asynchronous buffers and cannot retain `RequestLocal`
for use on background threads.

Optional deletion, readiness probing, and log-transition services
use `Optional<Capability>` consistently. Capability availability is independent;
broker code never downcasts to the native implementation.
The loader establishes the plugin context classloader for component and capability
calls. Providers must preserve that context when submitting work to executors
they do not own; the synchronous proxy cannot govern arbitrary future callbacks.

External engines have no native batch-coordinate cache. `DelayedFetch` hands
waiting to the engine's asynchronous fetch implementation when that cache is
absent. The native engine retains its original readiness probe. Kafka still
combines classic and diskless results and applies its response handling.

## Controller lifecycle shared by both providers

Both providers implement `DisklessTopicLifecycle`. The controller factory owns
the provider lifetime, and controller APIs depend only on the lifecycle SPI.
The native service borrows the shared control plane; it must not close a client
that the broker role may still use.

Each lifecycle implements exactly one typed contract. The controller factory
selects the request service or reconciler once; ControllerApis accepts only
`RequestDriven`, and the reconciler accepts only `MetadataDriven`:

| Mode | Creation and expansion | Deletion | Recovery |
| --- | --- | --- | --- |
| `RequestDriven` (native Inkless) | Provision after KRaft succeeds and before completing the response. Partition ranges exclude migrating partitions. | Complete storage deletion before deleting KRaft metadata. | Preserve the existing request-retry behavior. |
| `MetadataDriven` (Ursa) | Reconcile the committed topic layout and configuration. | Reconcile committed deletion and durably fence the old topic ID. | Retry across leadership changes, inventory managed topics, and sweep orphans by source revision. |

Both contracts share immutable topic identity and idempotent deletion.
`RequestDriven.ensurePartitions` supports explicit ranges on the migration path.
Its `ensureTopic` has no unused configuration or revision arguments.
`MetadataDriven.ensureTopic` receives the desired layout, configuration, and
source revision. Validation-only requests perform no storage operations.

Native Inkless does not yet expose a revision-aware catalog and durable
reconciliation protocol through its ControlPlane API. Its request-driven contract
therefore has no inventory or orphan-sweep operations. Moving native Inkless to metadata-driven recovery
requires a separate persistence change; the shared interface does not imply
that guarantee.

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
diskless.engine.class.name=org.apache.kafka.storage.diskless.UrsaStorageProvider
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
`diskless.engine.class.name` names a `DisklessStorageProvider`, not a
`DisklessEngine`. This changes the experimental PoC contract; existing PoC
configurations must select `UrsaStorageProvider` and rebuild their plugin.
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

The stateless provider has separate factory methods for a fully initialized
broker engine and controller lifecycle. Each returned component owns its resources;
Kafka closes it independently, releasing its classloader lease. A failed factory
call must close resources it opened. No partially initialized broker engine is
constructed on the controller. Only the active controller reconciles committed metadata. The copied
reconciler retains bounded concurrency, retries, metadata revisions, and orphan
recovery; its sweep interval is ten minutes. Retriable backend failures are
logged and metered, rather than treated as Kafka metadata replay faults.

Broker opens remain create-if-absent, so requests can race with controller
reconciliation. Both use the same topic ID, partition count, and metadata
revision. Deletion fences the immutable topic ID before retiring local handles under the
same lifecycle lock; a same-name replacement gets a different storage identity.
The Ursa engine also checks tracked partitions against a consistent Kafka metadata
image every 30 seconds and closes handles for invalid identities or partitions.
This fallback preserves persisted producer snapshots; durable deletion belongs to
the controller. It is not idle eviction or zone-owner reconciliation, and valid
partitions may remain cached on any broker that has served them.

## Source provenance and maintenance

The source baseline is UFK commit `706699788b`; the Inkless baseline is
`e9de1e2e9b`. The implementation and license headers are copied, not rewritten.

- `storage/ursa/src/main/java/org/apache/kafka/storage/diskless/` contains UFK's
  data/read path, producer state, and catalog lifecycle implementation.
- `UrsaDisklessEngine` is the new broker adapter. `UrsaStorageConfig` uses
  provider-local copies of UFK's configuration defaults instead of adding Ursa
  keys to Kafka's server config class.
- `UrsaStorageState` is final to satisfy this checkout's constructor-escape
  compiler check. Metadata access now uses the engine's UUID-aware snapshots;
  partition opening and writer initialization share the same topic configuration.
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
Consolidation and migration tests cover the native consolidation and log-transition adapters.
Engine tests cover task cancellation and resource closure after a handler fails;
broker tests cover exceptional plugin deletion results. Shared offset assertions
exercise partial success and empty batches against both native and isolated Ursa
engines. Snapshot tests cover metadata changes and same-name recreation. A proxy
contract test discovers optional capabilities and verifies their classloader context.
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
SPI now uses topic-ID-aware offset requests/results and immutable metadata snapshots.
The native offset handler also associates results by UUID, so distinct incarnations
of the same topic cannot overwrite each other's pending results.
