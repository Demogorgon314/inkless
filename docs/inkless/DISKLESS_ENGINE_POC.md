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

# Evaluate a diskless engine boundary

A record-level engine boundary is feasible for append, fetch, and offset lookup.
It can preserve Inkless's native implementation while letting a provider own its
log format, offsets, and producer state. An object-storage interface cannot do
that: it receives serialized objects after the native engine has chosen the WAL
format and metadata model.

This PoC extracts that data-plane boundary. It does **not** connect Ursa, replace
the controller's control plane, or establish production compatibility. A custom
engine must not be used with persistent application data on this branch.

## Evidence and scope

The inspected baselines are Inkless `e9de1e2e9b` and UFK `706699788b`.
Inkless declares `4.3.0-inkless-SNAPSHOT`. This review does not establish whether
Apache Kafka has merged any diskless implementation commits.

As inspected on September 22, 2026:

- [KIP-1150](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/345377898/KIP-1150+Diskless+Topics)
  is accepted.
- [KIP-1163](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/350783976/KIP-1163+Diskless+Core)
  remains under discussion and exposes byte-oriented `ObjectStorage` operations.
  Its design also includes local replica caches, tiered-storage integration, and
  transactions. Those obligations exceed UFK's RF=1, nontransactional model.
- [KIP-1164](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/350783984/KIP-1164+Diskless+Coordinator)
  remains under discussion. The coordinator owns more than object locations:
  it also owns ordering and producer/transaction metadata.

The inference from the source inspection is narrower than "replace three
handlers and the integration is complete." The handlers form a useful seam, but
other broker and controller operations still reach the native control plane.

## Implemented architecture

```text
ReplicaManager: existing classic/diskless routing and response aggregation
  |
  +-- DisklessEngine: append / fetch / createOffsetJob
        |
        +-- default: InklessDisklessEngine
        |     +-- existing AppendHandler
        |     +-- existing FetchHandler
        |     +-- existing FetchOffsetHandler
        |
        +-- configured provider: no native data handlers constructed

Controller, SharedState, deletion, retention, and storage initialization
  +-- existing native ControlPlane and StorageBackend (not replaced)
```

The default adapter calls the existing handlers and closes them. It does not
copy their algorithms. `ReplicaManager` retains its routing, ordered fetch
inputs, mixed-request aggregation, and existing purgatory. The native
consolidation path retains its specialized reader and offset handler.

`FetchOffsetHandler.Job` implements a small `OffsetJob` interface. This preserves
batching, cancellation, and delayed hybrid-read fallback without rewriting the
router. It also routes the diskless `OffsetsForLeaderEpoch` lookup through the
engine. A public upstream SPI should replace `TopicPartition` and the internal
`FileRecordsOrError` result with topic-ID-aware request/result types. Kafka can
then adapt those futures into its private purgatory job. The PoC deliberately
does not present this job interface as the final public API.

The SPI is temporarily in the existing Inkless module to avoid a build/module
reorganization during the experiment. A production SPI needs a Kafka-owned API
module and must not depend on the provider's implementation artifact.

## Configure the experiment

With no engine property, the native implementation remains the default.
An experimental provider has a public no-argument constructor and implements
`DisklessEngine`:

```properties
diskless.storage.system.enable=true
diskless.engine.class.name=com.example.ExampleEngine
diskless.engine.config.endpoint=example
```

The loader passes only `endpoint=example` to `configure`. It loads the class from
the broker classpath, closes it if configuration fails, and does not fall back
silently. These experimental properties are read from the original broker
configuration; they are not yet registered as public Kafka configuration keys.

`diskless.engine.class.path` and `diskless.engine.request.timeout.ms` are **not
implemented**. This branch still initializes native `SharedState`, storage, and
the control plane. Keeping the three native data handlers uninitialized does
not isolate a provider from that runtime dependency.

Custom engines reject managed replicas at construction. This also excludes
classic-to-diskless switching and consolidation, which depend on that setting.
DeleteRecords, topic deletion, retention, and lifecycle notifications remain
native and therefore do not act on custom-engine data. The test provider is a
routing fixture, not an in-memory Kafka implementation or an Ursa adapter.

## Complete the boundary before connecting Ursa

| Area | Evidence in this checkout | Required ownership |
| --- | --- | --- |
| Append and fetch | `ReplicaManager.appendRecords`, `fetchDisklessMessages` | Engine owns validation, ordering, idempotence, persistence, and read visibility. Broker owns authentication, authorization, quotas, and request routing. |
| Offset lookup | `DisklessFetchOffsetRouter`, `FetchOffsetHandler.Job` | Keep cancellation and hybrid fallback in the broker. Pass immutable topic IDs into the provider and define all special timestamp results. |
| DeleteRecords | `ReplicaManager.deleteRecords`, `DeleteRecordsInterceptor` | Add engine truncation with per-partition errors and low watermarks; do not keep truncating the native metadata for externally stored data. |
| Startup and shutdown | `SharedServer`, `BrokerServer`, `SharedState` | Select the provider before native storage/control-plane initialization. The native provider owns its background workers and resources. |
| Retention and garbage collection | `RetentionEnforcer`, `FileCleaner`, `TopicPurger` | The provider owns physical cleanup and orphan rules for its format. Kafka supplies topic policy. |
| Topic lifecycle | `ControllerApis` create/delete/create-partitions calls | Provide an idempotent asynchronous lifecycle SPI with recovery after controller changes. Metadata commit and storage readiness are different events. |
| Replicas and consolidation | `ConsolidationFetcherManager`, `InitDisklessLogManager`, direct control-plane accesses in `ReplicaManager` | Either support the agreed replica model or negotiate an explicit restricted mode. These cannot keep accessing native batch coordinates for Ursa data. |
| Metadata routing | `InklessTopicMetadataTransformer` | Kafka filters eligible brokers and listener endpoints. A provider may supply locality hints, subject to the same eligibility rules. |

### Lifecycle and fencing

The proposed two `void` controller methods lose asynchronous failure and retry
information. Post-commit callbacks alone also lose deletions that occur while a
controller is down. A useful contract includes:

- `ensureTopic(..., metadataRevision)` returning a future, with idempotent
  partition growth and config application.
- `deleteTopic(topicId, name)` returning a future and durably fencing that topic
  incarnation against late creates.
- A recovery mechanism: an owned-topic inventory/orphan sweep, or durable replay
  of lifecycle intents. Define how a newly active controller rejects stale work.
- Broker-side deletion fencing before closing cached partition handles. Closing
  a handle must not let an in-flight request recreate a deleted topic.

UFK already implements these concerns in `DisklessTopicLifecycle`,
`DisklessTopicLifecycleReconciler`, and `DisklessStorageEngine.fenceDeletedTopic`.
Reuse those implementations when connecting the controller, adapting their
metadata/config integration. Copying them into this data-plane-only PoC would
leave unused code and imply lifecycle behavior that is not wired up.

`onPartitionsAssigned` is not enough to open handles: diskless requests can be
served by brokers other than the conventional partition leader. Providers need
lazy opening from authoritative topic metadata, plus explicit stop/delete
semantics. An assignment callback must not grant writes after a topic fence.

### Transactions, context, and asynchronous contracts

`supportsTransactions() == false` is a capability declaration, not transaction
support. Kafka must reject incompatible requests/configuration consistently.
A transaction-capable provider needs marker handling, producer fencing, last
stable offsets, aborted-transaction metadata, and coordinator integration.
Cross-classic/diskless transactions need an agreed protocol; an append boolean
cannot supply it.

`MetadataRequest.RackId` is metadata-request context. It does not automatically
appear on subsequent Produce requests, which may reach another broker or use
another connection. Define where each operation gets its rack, allow unknown
racks, and keep client-ID parsing as an explicit compatibility convention.
UFK's `Writer.write(records, zone)` needs that decision before it can be adapted.
`brokerId` belongs in engine initialization; request identity, listener, and
deadline belong in per-request context. Do not add context fields that callers
cannot populate reliably.

The final asynchronous contract must specify buffer lifetime, synchronous
exceptions, failed futures, per-partition failures, fetch byte budgets and
ordering, timeout behavior, and late completion. Timing out an append does not
undo its commit. Canceling a response must not free buffers still used by the
provider. `RequestLocal` is thread-confined and cannot be retained for worker
threads. None of these guarantees follows from `CompletableFuture` alone.

### Reuse UFK without importing its broker bypass

UFK's `Writer.write` and `Reader.fetch` already use compatible Kafka record and
response types. Its `Reader.listOffsets` needs an adapter for this checkout's
offset result and cancellation model. Reuse the Ursa storage implementation and
its isolated loader; keep Inkless's broker routing as the single routing layer.
Do not copy `DisklessStorageReplicaManagerSupport` beside it and maintain two
independent dispatch paths.

The isolated provider runtime must carry Lakestream/Oxia dependencies. Reuse
UFK's loader/classloader code after settling the API module boundary; adding
Ursa jars to the broker classpath would not validate that design. No Ursa code
or dependencies are copied by this first experiment.

## Discuss with the KIP authors

Propose a record-level engine extension point **above** the object-store SPI.
Keep `ObjectStorage` as a dependency of the default engine. The default engine
retains its WAL builder and diskless coordinator client; an external engine
owns its own format and metadata.

First agree on whether external engines may own ordering and producer state,
and which replica/transaction semantics every engine must implement. Then
separate a small stable data API from the controller lifecycle API. The native
design can remain the default implementation, but the public compatibility and
ownership contract changes; it is not accurate to promise no KIP design change.

Classic-to-diskless migration is outside the current KIP-1163 topic-config scope.
Keep `initLog` out of the first public engine API until its retry, producer-state,
and fencing semantics are designed. Rack-aware broker selection can likewise
remain a broker policy unless a demonstrated provider requirement needs a hook.

## Validation

The targeted tests cover existing native handlers, mixed/hybrid broker routing,
and the offset router. A new broker test loads a configured provider, routes
append/fetch/ListOffsets to it, checks shutdown, and asserts that no native data
handler is constructed. Loader tests cover config isolation and failed-startup
cleanup. These prove routing and resource ownership at this seam, not durable
storage, multi-broker recovery, or Ursa compatibility.

```bash
./gradlew :storage:inkless:test \
  --tests 'io.aiven.inkless.engine.DisklessEnginesTest' \
  --tests 'io.aiven.inkless.produce.AppendHandlerTest' \
  --tests 'io.aiven.inkless.consume.FetchHandlerTest' \
  --tests 'io.aiven.inkless.consume.FetchOffsetHandlerTest' \
  :core:test --tests 'kafka.server.ReplicaManagerInklessTest' \
  --tests 'kafka.server.DisklessFetchOffsetRouterTest'
```
