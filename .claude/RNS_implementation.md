# `RNS.java` — Implementation Plan

**Companion to:** `.claude/RNS_analysis.md`
**Baseline:** commit `4ec4609a`, branch `feature/reticulum-refactoring`
**Target:** `src/main/java/org/qortal/network/reticulum/` — `RNS.java` 2610 L → ~250 L facade + 8 collaborators
**Build constraints:** Java 11 (`pom.xml` `<release>11</release>`), JUnit 5 + Mockito 5.10 (test scope), Lombok

---

## 0. Ground rules

1. **One phase = one commit.** Every phase compiles the whole tree and is independently revertable. Never mix a move with an edit.
2. **Comment preservation is mandatory.** The concurrency comments in `RNS.java` are the only record of why several obvious simplifications are wrong. Carry them verbatim to the new home. §10 lists the ones that must survive, with a grep check.
3. **No behaviour change unless listed.** §9 is the complete register of intended behaviour changes. Anything not in that list that changes behaviour is a bug in the refactor.
4. **Side effects stay outside locks.** `shutdownChannel()`, `makePeerUnavailable()`, `closeIfActive()` and any Reticulum call must never run while holding a registry lock — `removeLinkedPeer`'s comment (RNS.java:1960) documents the deadlock this avoids.
5. **Verification after every phase:** `mvn -q -DskipTests compile` must pass with zero new warnings; from phase 4 on, `mvn -q -Dtest='RNS*Test,KnownPeerStoreTest' test` must pass.

---

## 1. Target layout

```
src/main/java/org/qortal/network/
├── reticulum/
│   ├── RNS.java                    ~250 L  facade: singleton, lifecycle, public API
│   ├── RNSCommon.java              ~50 L   unchanged (PeerAspect, PeerMetaType, constants)
│   ├── ReticulumPeer.java          1633 L  moved only — out of scope for this plan
│   ├── ReticulumPeerAddress.java   71 L    moved only
│   ├── RNSConfigWriter.java        ~90 L   Jinjava render + fallback config          [new]
│   ├── RNSAnnounceCodec.java       ~170 L  QAN1/QGW1 codec, pure static              [new]
│   ├── RNSGatewayManager.java      ~200 L  advertise host, dynamic backbone client   [new]
│   ├── RNSPeerRegistry.java        ~260 L  the 4 collections, sole mutator           [new]
│   ├── KnownPeerStore.java         ~80 L   per-aspect hash persistence               [new]
│   ├── ReconnectPolicy.java        ~90 L   per-aspect failure counts + backoff       [new]
│   ├── RNSAspectRunner.java        ~340 L  one instance per aspect: loop+executors   [new]
│   └── RNSPeerPruner.java          ~170 L  prunePeers passes, isUnreachable          [new]
└── (unchanged) Peer.java, Network.java, NetworkData.java, task/Reticulum*Task.java …
```

`org.qortal.network.task.ReticulumMessageTask` / `ReticulumPingTask` stay where they are (they sit with the other `Task` implementations); they only need an import update.

**Line budget:** 2610 → ~1700 across 10 files, of which ~250 is the facade. The reduction comes from ~380 L of dead weight (phase 1), ~280 L of BASE/DATA de-duplication (phase 8) and ~150 L of dead public API (phase 1).

---

## 2. Phase table

| # | Commit | Work | Risk | Δ `RNS.java` |
|---|---|---|---|---|
| 1 | ✅ `12873ec5` | commented-out blocks, unused fields/imports/locals, zero-caller public methods | none | −460 (2610→2150) |
| 2 | ✅ `2aa0d14b` | pure `git mv` + package/import updates, no logic change | low (wide diff) | 0 |
| 3 | ✅ `2b90404f` | drop ~100 generated accessors, keep 6 | very low | +6 |
| 4 | ✅ `3579c18b` | §5.1, §5.2, §5.3, §5.5 (gateway map), §5.6 from the analysis | very low | +51 |
| 5 | ✅ `9e9d8737` | `RNSAnnounceCodec` + 18 tests, all passing | very low | −110 (→2046) |
| 6 | ✅ `92eec531` | `KnownPeerStore` ×2 + `RNSConfigWriter` | low | −170 (→1876) |
| 7 | ✅ `9b0b2a62` | `RNSGatewayManager` + dialling off the announce thread (§5.4) | low | −175 (→1701) |
| 8 | ✅ `9e234e80` | `RNSPeerRegistry` + 16 tests | **medium** | −132 (→1569) |
| 9 | ✅ `cda0c7a0` | `RNSPeerPruner` + 14 tests | low–medium | −132 (→1437) |
| 10 | ✅ `6bf85e93` + `b29ea96d` | `ReconnectPolicy` + `RNSAspectRunner`, BASE then DATA | **highest** | −583 (→854) |
| 11 | ✅ `993f2351` | 10 ms → 50 ms tick, INFO → DEBUG | low | ~0 |

Phases 1–7 are ~60 % of the reduction at near-zero risk and can ship before any decision on 8–10.

---

## 3. Phase 1 — dead weight (`−520 L`)

### 3.1 Commented-out blocks (line refs at `4ec4609a`)

| Lines | Content |
|---|---|
| 1743–1840 | `RNSProcessor` inner class |
| 996–1018 | `broadcastOurChain`, `buildNewTransactionMessage`, `buildGetUnconfirmedTransactionsMessage` |
| 1891–1902 | `makePeerAvailable` + getter note |
| 1938–1944 | `makePeerUnavailable` |
| 1965–1971 | `getLinkedPeers` |
| 2110–2116 | `getIncomingPeers` / `getImmutableIncomingPeers` |
| 2572–2577 | second `removePeer` |
| 2149–2159 | repository block inside `peerMisbehaved` (method itself goes, see 3.3) |
| 2179, 2192–2195, 2271–2278 | commented ping/prune fragments inside `prunePeers` |
| import block | ~25 commented import lines (15–19, 24–26, 28, 31–33, 36, 39–40, 43, 54, 63–64, 66, 72–73, 77–79, 88–89, 93, 106–107) |

### 3.2 Unused declarations

- Fields: `MAX_PEERS` (126), `PRUNE_INTERVAL` (130), `reticulumMaxNetworkThreadPoolSize` (228 — the constructor reads `Settings` directly), `BROADCAST_INTERVAL` (239).
- Imports: `SelectionKey` (60), `AtomicLong` (76), `Predicate` (84), `BlockData` (100), `TransactionData` (102).
- Dead locals: `initiatorActivePeerList` (2185, 2324), `incomingPeerList = this.incomingPeers` (2270).
- Duplicated javadoc: first block of 1191–1205 (describes the superseded QGW1-only format), first block of 2007–2020 (attached to the wrong method).

### 3.3 Public methods with zero callers in the tree

Verified with `grep -rn "<name>" --include="*.java" src/main src/test` — each appears only at its own definition (plus commented references):

| Method | Note |
|---|---|
| `sendCloseToRemote(Link)` | `shutdown()` calls `ReticulumPeer.sendCloseToRemote`, not this one |
| `closePacketDelivered`, `packetTimedOut` | only reachable from the above |
| `clientDisconnected`, `serverPacketReceived` | referenced only from commented callbacks |
| `buildHeightOrChainTipInfo(ReticulumPeer)` | all callers use `Network.getInstance().buildHeightOrChainTipInfo` |
| `onPingMessage(ReticulumPeer, Message)` | `Network.onPingMessage` handles both peer types |
| `peerMisbehaved(Peer)` | all callers use `Network.getInstance().peerMisbehaved` |
| `findPeerByLink`, `findPeerByDestinationHash` | no callers (also the INFO-per-match loggers from §6.4) |
| `getOurNodeId`, `getOurPublicKey` | callers use `Network`/`NetworkData` |
| `getAllKnownCorePeers`, `getAllKnownDataPeers` | only `getAllKnownPeers` is used (Network.java:807) |
| `maybeRecoverInstance` | empty TODO stub |

Cascade deletions once those are gone: `BROADCAST_CHAIN_TIP_DEPTH`, imports `Repository`, `RepositoryManager`, `BlockSummaryData`, and the `Class.forName` + `ClassNotFoundException` handler (§5.8).

**Keep** `DataException` in imports — `prunePeers()` must retain `throws DataException` (see §8.3).

**Guard:** this deletes public methods. Re-run the full-tree grep before deleting each one; `mvn -q -DskipTests compile` plus `mvn -q -DskipTests test-compile` is the acceptance gate.

---

## 4. Phase 2 — package move

```bash
mkdir -p src/main/java/org/qortal/network/reticulum
git mv src/main/java/org/qortal/network/{RNS,RNSCommon,ReticulumPeer,ReticulumPeerAddress}.java \
       src/main/java/org/qortal/network/reticulum/
```

Then:

1. `package org.qortal.network.reticulum;` in all four files.
2. Add to each moved file the imports it now needs from `org.qortal.network`: `Peer`, `PeerAddress`, `PeerCtor`, `PeerAddressCtor`, `PeerSendManager` (ReticulumPeer), `Network`, `NetworkData`.
3. Update importers — `org.qortal.network.RNSCommon.PeerMetaType` → `org.qortal.network.reticulum.RNSCommon.PeerMetaType` in:
   `Peer.java:40`, `IPPeer.java:41`, `Network.java:45`, `NetworkData.java:16`, `data/network/PeerData.java:6`.
4. Add `import org.qortal.network.reticulum.RNS;` / `ReticulumPeer` to: `controller/Controller.java`, `controller/arbitrary/ArbitraryDataFileManager.java`, `controller/arbitrary/PeerMessage.java`, `api/resource/PeersResource.java`, `api/model/ConnectedPeer.java`, `network/Network.java`, `network/NetworkData.java`, `network/PeerSendManager.java`, `network/PeerSendManagement.java`, `network/task/ReticulumMessageTask.java`, `network/task/ReticulumPingTask.java`, `src/test/java/org/qortal/test/network/RNSNetworkTest.java`.

### 4.1 Access-modifier survey (done — no surprises)

- `Peer` is an **interface**, so `NETWORK`/`NETWORKDATA` and every method are implicitly `public`. Cross-package use is fine.
- `ReticulumPeer`'s package-private fields (`peerLink`, `peerBuffer`, `channel`, `peerAddress`, `peerLinkHash`, `receiveStreamId`, `sendStreamId`) are **not** touched from outside `RNS`/`ReticulumPeer` — verified by grep. Both move together.
- `RNS.confirmPeerHash` and `RNS.markPeerForImmediateRemoval` are package-private and called from `ReticulumPeer` — same package after the move, so they stay package-private.
- `PeerFactory` / `PeerAddressFactory` scan with `new Reflections("org.qortal.network")`, which is a **prefix** scan and includes subpackages, so the `@PeerCtor("destination-hash")` / `@PeerCtor("link")` / `@PeerAddressCtor("destination-hash")` registrations keep working. Confirm at runtime with a node start (§11.2), not just by compiling — this is reflection, the compiler cannot catch it.

**Alternative if step 3 is unwanted:** keep `RNSCommon.java` in `org.qortal.network` (it serves the IP path too via `PeerMetaType`). Costs one cross-package import in the reticulum classes instead of five in the IP classes. The plan above moves it, for the "all reticulum files in one folder" rule from the analysis §10.

---

## 5. Phase 3 — `@Data` → explicit getters

Delete `@Data`; keep `@Slf4j`. Add exactly these:

```java
@Getter private Identity serverIdentity;      // ReticulumPeer:654
@Getter private Destination baseDestination;  // ReticulumPeer:903
@Getter private Destination dataDestination;  // internal only — keep package-private getter
@Getter private volatile boolean isShuttingDown = false;   // → isShuttingDown()
private volatile boolean meshStarted = false;              // explicit isMeshStarted() already exists
```

Snapshot accessors become hand-written (they are the only list API that survives):

```java
public List<ReticulumPeer> getImmutableLinkedPeers()   { return registry.linked(); }
public List<ReticulumPeer> getImmutableIncomingPeers() { return registry.incoming(); }
```

Everything else that `@Data` generated goes, in particular:

- **All setters.** `setLinkedPeers`, `setImmutableLinkedPeers`, `setShuttingDown`, `setReticulum`, … — these could bypass every invariant in `addLinkedPeer`.
- **`getLinkedPeers()` / `getIncomingPeers()`** — handing out the live `synchronizedList` is the direct cause of §5.3. Removing the accessor fixes the class of bug, not just the one call site.
- **`toString()` / `equals()` / `hashCode()`.** Note `Controller.java:578` has a commented `rns.toString()`; leave it commented and do not restore a `toString`.

**Lombok naming trap:** keep the field named `isShuttingDown` (not `shuttingDown`) so `@Getter` still emits `isShuttingDown()` and `ReticulumPeer:498` / `ReticulumPeer:679` compile unchanged.

The full external surface after this phase (18 members — verified by grep over `src/main`):

```
getInstance  start  shutdown  prunePeers  isMeshStarted  isShuttingDown
broadcast  onPeersV2Message  isUnreachable  getAllKnownPeers  getActiveDataPeers
getImmutableLinkedPeers  getImmutableIncomingPeers  getServerIdentity  getBaseDestination
triggerImmediateAnnounce  markPeerForImmediateRemoval  confirmPeerHash  dedupIncomingPeerByIdentity
```

---

## 6. Phase 4 — correctness fixes

### 6.1 `shutdown()` bitwise `&` (RNS.java:1052)

```java
-            if (nonNull(pl) & (pl.getStatus() == ACTIVE)) {
+            if (nonNull(pl) && (pl.getStatus() == ACTIVE)) {
```

An NPE here aborts `shutdown()` before the linked-peer loop, the executor shutdown and `exitHandler()`.

### 6.2 Atomic snapshot rebuild on the remove paths

`removeLinkedPeer` (1952) and `removeIncomingPeer` (2103) mutate and republish without the lock that `addLinkedPeer` holds. Fix — and note carefully what stays **outside** the lock:

```java
public void removeLinkedPeer(ReticulumPeer peer) {
    peer.shutdownChannel();               // outside: touches Reticulum
    synchronized (this.linkedPeers) {     // inside: mutation + snapshot, nothing else
        this.linkedPeers.remove(peer);
        this.immutableLinkedPeers = List.copyOf(this.linkedPeers);
    }
    peer.makePeerUnavailable();           // outside: acquires Network's peer-list locks
}
```

The comment at 1954–1961 explaining *why* `makePeerUnavailable()` runs with no RNS lock held must move with it. Same shape for `removeIncomingPeer` (its `closeIfActive(peer)` also stays outside).

### 6.3 Unsynchronised iteration

```java
public List<ReticulumPeer> getNonActiveIncomingPeers() {
    List<ReticulumPeer> result = new ArrayList<>();
    for (ReticulumPeer p : getImmutableIncomingPeers()) {   // snapshot, not the live list
        Link pl = p.getPeerLink();
        if (pl == null || pl.getStatus() != ACTIVE) result.add(p);
    }
    return result;
}
```

Same substitution in `shutdown()` (1050, 1058): iterate `getImmutableIncomingPeers()` / `getImmutableLinkedPeers()`.

Also drop the pointless `Collections.synchronizedList` wrapper in `getActiveImmutableLinkedPeers()` (1852) and in `getNonActiveIncomingPeers()` — these are single-caller snapshots; the wrapper only implies a thread-safety contract that does not exist.

### 6.4 Half-built singleton (§5.6)

Keep the constructor non-throwing (`getInstance()` must not blow up in a static initialiser), but make `start()` refuse to run:

```java
public void start() {
    if (reticulum == null) {
        log.error("Reticulum stack unavailable (see construction error above) — mesh will not start");
        return;   // meshStarted stays false; every consumer already guards on isMeshStarted()
    }
    ...
}
```

`Network.java:460` and every consumer already guard on `isMeshStarted()`, so this degrades cleanly instead of NPE-ing at `reticulum.getStoragePath()` (399).

### 6.5 Unbounded maps (§5.5)

- `recentGatewayAttempts` → phase 7, evicted inside `RNSGatewayManager` (`removeIf(age > 2 × GATEWAY_COOLDOWN)` once per call, plus a hard cap of 256).
- `pendingLinkFailureMs` / `pendingFailureCount` → phase 10, `ReconnectPolicy.evictOlderThan(24 h)` called once per reconnect cycle.

---

## 7. Phases 5–7 — the stateless extractions

### 7.1 `RNSAnnounceCodec` (phase 5)

Pure, static, no Reticulum/Settings/Controller dependency — the caller supplies the version string and the gateway host, so the codec is constructible in a unit test.

```java
public final class RNSAnnounceCodec {
    static final byte[] QAN_MAGIC = { 'Q', 'A', 'N', '1' };
    static final byte   TLV_VERSION = 0x01;
    static final byte   TLV_GATEWAY = 0x02;
    static final byte[] QGW_MAGIC = { 'Q', 'G', 'W', '1' };   // legacy, decode-only
    private static final int QGW_MIN_LEN = QGW_MAGIC.length + 1 + 2;

    private RNSAnnounceCodec() { }

    /** QAN1 container: version record always, gateway record when host != null and port in 1..65535. */
    public static byte[] encode(String version, String gatewayHost, int gatewayPort);

    /** Never returns null. Falls back to legacy QGW1 when the QAN1 magic is absent. */
    public static AnnounceInfo decode(byte[] appData);

    /** "x.y.z[-hash]", with or without the "qortal-" prefix → 3×16-bit packed long; 0 if unparseable. */
    public static long parseVersionToLong(String versionString);

    /** Rejects null/empty, "localhost", 127.*, ::1, and single-label names. */
    public static boolean isUsableAdvertiseHost(String host);

    public static final class AnnounceInfo {          // NOT a record — pom targets Java 11
        private final String version;                 // nullable
        private final String gatewayHost;             // nullable
        private final int gatewayPort;                // 0 = absent
        // getters + equals/hashCode/toString (tests compare instances)
    }
}
```

Wire-format invariants to carry over verbatim from `buildAnnounceAppData`/`buildGatewayValue`/`decodeAnnounceAppData`:

- version value truncated to 255 bytes; gateway host must encode to 1..252 bytes (so `1 + n + 2 ≤ 255`); port 1..65535.
- decode stops at a truncated record, skips unknown TLV types, tolerates `null`/short input.
- gateway TLV body is byte-identical to the QGW1 body minus its magic — that is what keeps old peers parseable.

`RNS` then becomes:

```java
private byte[] buildAnnounceAppData() {
    return RNSAnnounceCodec.encode(
            Controller.getInstance().getVersionStringWithoutPrefix(),
            gatewayManager.getAdvertiseHost(),   // null when disabled/unusable — codec omits the record
            TARGET_PORT);
}
```

Note the shift: the "is the gateway feature enabled" decision (`getReticulumAnnounceGateway()` && `getReticulumIsGateway()`) moves into `RNSGatewayManager.getAdvertiseHost()`, which returns `null` when disabled. Behaviour identical, `Settings` reads confined to one class.

**Tests** — `src/test/java/org/qortal/test/network/RNSAnnounceCodecTest.java` (JUnit 5):

| Test | Asserts |
|---|---|
| `roundTripVersionOnly` | `decode(encode("6.1.9-abc", null, 0))` → version set, gwPort 0 |
| `roundTripVersionAndGateway` | both fields survive, port big-endian |
| `legacyQgw1Decodes` | hand-built QGW1 buffer → host/port set, version null |
| `truncatedRecordStopsCleanly` | `encode(...)` sliced short → no exception, partial fields |
| `unknownTlvIsSkipped` | injected type `0x7F` between version and gateway → both still decode |
| `oversizedHostOmitsGatewayRecord` | 253-byte host → no gateway TLV, version still present |
| `portBounds` | 0 and 65536 → gateway record omitted |
| `nullAndEmptyAppData` | `decode(null)` / `decode(new byte[0])` → all-absent info, no throw |
| `parseVersion` | `"6.1.9"`, `"qortal-6.1.9-abc"` equal; `"garbage"` → 0; `40000.0.0` → 0 |
| `isUsableAdvertiseHost` | localhost / 127.0.0.1 / ::1 / `dev-vm` rejected; `a.example.com`, `1.2.3.4` accepted |

This is the first executable test of the announce wire format — today none of it is reachable without constructing `RNS` (which builds a Reticulum stack and five thread pools).

### 7.2 `KnownPeerStore` + `RNSConfigWriter` (phase 6)

```java
final class KnownPeerStore {
    private final Path file;
    private final Set<String> confirmed = ConcurrentHashMap.newKeySet();
    private final Set<String> loaded    = ConcurrentHashMap.newKeySet();

    KnownPeerStore(Path storagePath, String fileName);

    void load();                       // tolerant: unreadable/missing file → no-op + log
    void save();                       // writes confirmed, or loaded when confirmed.isEmpty()
    boolean confirm(String hashHex);   // true when newly added; caller decides whether to save()
    Set<String> reconnectTargets();    // new HashSet<>(confirmed) + loaded — a copy, always
    boolean hasLoadedHashes();         // drives the start() announce-timer seeding
}
```

Instantiated twice in `RNS.start()`:

```java
this.baseStore = new KnownPeerStore(reticulum.getStoragePath(), "known_peer_hashes.txt");
this.dataStore = new KnownPeerStore(reticulum.getStoragePath(), "known_data_peer_hashes.txt");
```

Preserve exactly: the "prefer confirmed, fall back to loaded" save rule (2391), the two separate sets (loaded entries are never written back directly, which is how stale entries age out), and the `reticulum == null` early return.

`RNSConfigWriter.write(Path configDir)` is a straight lift of `initConfig` — Jinjava context build, `Files.deleteIfExists` before write (the truncation fix at 542–547 has its comment carried over), fallback to the packaged default config on any exception. Static method, no state.

### 7.3 `RNSGatewayManager` (phase 7)

```java
final class RNSGatewayManager {
    private final String appName;
    private final int targetPort;
    private volatile String localFqdn;
    private volatile String advertiseHost;
    private volatile boolean advertiseHostResolved;
    private final Map<String, Instant> recentAttempts = new ConcurrentHashMap<>();
    private final ExecutorService dialExecutor;   // 1 thread, queue 8, DiscardPolicy, "RNS-GatewayDial"

    String getAdvertiseHost();                    // null when disabled or unusable; logs once
    void maybeAddDynamicGateway(String host, int port);
    void forceBackboneReconnect();                // iterates BackboneClientInterface, forceReconnect()
    void shutdown();
}
```

**The §5.4 fix.** `maybeAddDynamicGateway` keeps every *cheap* check inline on the caller's thread (usable-host, self-skip via FQDN and advertised name, cooldown stamp, existing-interface scan, initiator cap) so ordering and dedup stay deterministic; only the blocking tail is submitted:

```java
    dialExecutor.execute(() -> {
        try {
            BackboneClientInterface iface = new BackboneClientInterface();
            ... setInterfaceName/TargetHost/TargetPort/Enabled/IfacNetName/IfacNetKey ...
            if (!InterfaceUtils.initIFac(iface)) { log.warn(...); return; }
            Transport.getInstance().getInterfaces().add(iface);
            iface.launch();                       // TCP connect — no longer on the announce thread
        } catch (Exception e) { log.warn(...); }
    });
```

That removes the TCP dial from `QAnnounceHandler.receivedAnnounce` (Reticulum's announce-delivery thread) and lets the `@Synchronized` on the handler go — the analysis notes it does not serialise BASE against DATA anyway, since the two handler instances hold separate Lombok `$lock`s.

Cooldown map eviction (§5.5) goes at the top of `maybeAddDynamicGateway`:

```java
    recentAttempts.values().removeIf(t -> Duration.between(t, now).compareTo(GATEWAY_COOLDOWN.multipliedBy(2)) > 0);
```

---

## 8. Phases 8–10 — the stateful extractions

### 8.1 `RNSPeerRegistry` (phase 8)

Sole owner of the four collections. Every mutation takes one lock and republishes the snapshot **inside** it; every read returns the snapshot, never the live list.

```java
final class RNSPeerRegistry {
    private final Object linkedLock = new Object();
    private final List<ReticulumPeer> linked = new ArrayList<>();          // guarded by linkedLock
    private volatile List<ReticulumPeer> linkedSnapshot = List.of();
    private final Object incomingLock = new Object();
    private final List<ReticulumPeer> incoming = new ArrayList<>();        // guarded by incomingLock
    private volatile List<ReticulumPeer> incomingSnapshot = List.of();

    /** false when a peer with the same destination hash is already tracked (caller closes the loser). */
    boolean addLinked(ReticulumPeer peer);
    void removeLinked(ReticulumPeer peer);
    /** Returns superseded same-identity+aspect peers, already unlinked; caller runs their side effects. */
    List<ReticulumPeer> addIncoming(ReticulumPeer peer);
    void removeIncoming(ReticulumPeer peer);
    List<ReticulumPeer> duplicateIncomingByIdentity(ReticulumPeer keep);

    List<ReticulumPeer> linked();
    List<ReticulumPeer> incoming();
    List<ReticulumPeer> activeLinked();                       // link ACTIVE && !deleteMe
    List<ReticulumPeer> activeLinked(PeerAspect aspect);
    List<ReticulumPeer> activeIncoming(PeerAspect aspect);
    List<ReticulumPeer> nonActiveIncoming();
    Set<String> activeIncomingHashes(PeerAspect aspect);      // hashFromNameAndIdentity, precomputed once
    boolean isTracked(byte[] destinationHash, PeerAspect aspect);
}
```

Two design decisions worth stating explicitly:

- **Side effects move out.** `addIncoming` currently calls `shutdownChannel()` and `closeIfActive()` on evicted peers *while holding the list monitor*. The registry now returns the evicted peers and `RNS` runs those calls after the lock is released. Narrower lock, same outcome, and it keeps rule 4 from §0.
- **`activeIncomingHashes(aspect)` is a registry method** because it is the O(1) dedup set that the BASE reconnect path already precomputes (RNS.java:708–720) and the DATA path lacks. Putting it here is what lets phase 10 give both aspects the same behaviour for free.

**Testability caveat, stated honestly:** `RNSPeerRegistry` cannot be unit-tested cheaply — `ReticulumPeer`'s constructors call `initPeerLink()`, which sends a LINKREQUEST through `Transport`. Mockito is available (test scope) and `ReticulumPeer` is non-final, so `mock(ReticulumPeer.class)` with stubbed `getDestinationHash()`/`getPeerLink()`/`getPeerAspect()` covers the add/remove/dedup logic without touching Reticulum. Do that for `addLinked` dedup, `removeLinked` snapshot atomicity and `activeIncomingHashes`; do not attempt an end-to-end registry test.

### 8.2 `ReconnectPolicy` (phase 10 prerequisite)

```java
final class ReconnectPolicy {
    private static final long BASE_BACKOFF_MS = 60_000L;
    private static final long MAX_BACKOFF_MS  = 30 * 60_000L;
    private final Map<String, Long>    lastFailureMs = new ConcurrentHashMap<>();
    private final Map<String, Integer> failureCount  = new ConcurrentHashMap<>();

    void recordFailure(String hashHex);
    boolean isBackingOff(String hashHex);      // now - lastFailure < backoffMs(hashHex)
    long backoffMs(String hashHex);            // 60s, 120s, 240s … capped at 30 min
    void clear(String hashHex);                // on confirmed ACTIVE
    void evictOlderThan(long ageMs);           // §5.5 — called once per reconnect cycle
}
```

One instance **per aspect**. Today `pendingFailureCount` is shared between aspects while the time maps are split, and `clearPendingFailure` clears both time maps — but BASE and DATA destination hashes are distinct (different aspect string in `hashFromNameAndIdentity`), so an entry can only ever exist in one map. Per-aspect instances are therefore behaviour-preserving. Keep the comment at 156–164 explaining why the exponential backoff exists (PENDING link → `expirePath()` cull cascade).

### 8.3 `RNSPeerPruner` (phase 9)

`prunePeers()` (155 L, four unrelated passes) becomes:

```java
final class RNSPeerPruner {
    RNSPeerPruner(RNSPeerRegistry registry,
                  Consumer<ReticulumPeer> removeLinkedPeer,
                  Consumer<ReticulumPeer> removeIncomingPeer,
                  BiConsumer<String, PeerAspect> recordPendingFailure);

    void prune();                                  // logs before/after counts, calls the four passes
    private void pruneInitiatorPeers();            // timed-out / unreachable-ACTIVE / CLOSED / stuck-PENDING
    private void pruneNonActiveIncoming();
    private void dedupActiveIncomingByIdentity();
    private void pruneSilentActiveIncoming();
    static boolean isUnreachable(ReticulumPeer p); // boxed Boolean → primitive (§5.7)
}
```

The pruner decides *which* peers go; the two `Consumer` callbacks are `RNS::removeLinkedPeer` /
`RNS::removeIncomingPeer`, so every side effect (`shutdownChannel`, `closeIfActive`,
`makePeerUnavailable`) stays in `RNS` and rule 4 of §0 holds without the pruner knowing about
Reticulum or `Network` at all. `recordPendingFailure` is a new aspect-keyed overload on `RNS`
delegating to the existing `(String, ConcurrentHashMap)` one — the pruner no longer needs to know
that the two aspects use separate time maps, which is what `ReconnectPolicy` (§8.2) will absorb in
phase 10.

Each pass takes one snapshot at its start instead of recomputing `getActiveImmutableLinkedPeers()` / `getNonActiveIncomingPeers()` seven times per cycle (§6.3 of the analysis).

`RNS.prunePeers()` stays as a one-line facade and **must keep `throws DataException`**: `Controller.java:1004` has a `catch (DataException e)` around the call, and Java rejects a catch clause for a checked exception that cannot be thrown. Changing that signature turns a refactor into a Controller edit for no gain.

`isUnreachable` returning `boolean` instead of `Boolean` changes `ConnectedPeer.java:185` (`!RNS.getInstance().isUnreachable(rnsPeer)`) not at all — auto-unboxing simply disappears.

### 8.4 `RNSAspectRunner` (phase 10)

One class, instantiated twice; this is where the ~600 duplicated lines collapse to ~340 and where DATA inherits BASE's robustness.

```java
final class RNSAspectRunner {
    RNSAspectRunner(PeerAspect aspect,
                    String aspectName,              // "qortal.core" | "qortal.qdn"  (use CORE_ASPECT/QDN_ASPECT)
                    Destination destination,
                    int minDesiredPeers,
                    int messageTaskType,            // Peer.NETWORK | Peer.NETWORKDATA
                    KnownPeerStore store,
                    RNSPeerRegistry registry,
                    ReconnectPolicy policy,
                    RNSGatewayManager gateways,
                    ExecutorService workerPool,
                    Supplier<byte[]> appDataSupplier,
                    BiConsumer<byte[], Identity> peerFactory,   // createLinkedPeerFromIdentity, per aspect
                    BooleanSupplier shuttingDown);

    void start();                        // names the thread "rnsMesh-" + aspect
    void shutdown();                     // interrupt + join(5s) + executor shutdown/awaitTermination(2s)
    void triggerImmediateAnnounce();     // now - interval + 5s
    void seedAnnounceTimer();            // full window on first start, 15s when the store has loaded hashes

    private void run();                  // while(!shuttingDown && !interrupted): drain, announce, reconnect, sleep
    private void drainPeerTasks(long now);
    private void announceTick(long nowMs);
    private void reconnectTick(long nowMs);
    private boolean watchdog(String label, AtomicLong startedMs, AtomicReference<Future<?>> future,
                             ThreadPoolExecutor exec, long timeoutMs);   // written once, used 4×
}
```

Construction in `RNS.start()`:

```java
this.baseRunner = new RNSAspectRunner(PeerAspect.BASE, CORE_ASPECT, baseDestination,
        MIN_DESIRED_CORE_PEERS, Peer.NETWORK, baseStore, registry, basePolicy, gatewayManager,
        rnsWorkerPool, this::buildAnnounceAppData, this::createLinkedPeerFromIdentity, this::isShuttingDown);
this.dataRunner = new RNSAspectRunner(PeerAspect.DATA, QDN_ASPECT, dataDestination,
        MIN_DESIRED_DATA_PEERS, Peer.NETWORKDATA, dataStore, registry, dataPolicy, gatewayManager,
        rnsWorkerPool, this::buildAnnounceAppData, this::createLinkedDataPeerFromIdentity, this::isShuttingDown);
```

`maybeAnnounce` collapses from two near-identical `if` blocks to one:

```java
private void announce() {
    int count = registry.activeLinked(aspect).size();
    if (count > minDesiredPeers) return;             // see §9 item 4 for the <= → > change
    long t0 = System.currentTimeMillis();
    destination.announce(appDataSupplier.get());
    long ms = System.currentTimeMillis() - t0;
    log.info("{} announce attempt completed in {}ms", aspect, ms);
    if (ms > 5_000) log.warn("{} announce took {}ms — possible jobsLock contention", aspect, ms);
}
```

**Migration order inside phase 10** (do not do this as one edit):

1. Write `RNSAspectRunner` with BASE semantics only; wire BASE to it; delete `runBaseLoop`. Verify a node runs for ≥ 1 h with BASE peers connecting.
2. Wire DATA to a second instance; delete `runDataLoop`, the four DATA executors, the eight DATA volatiles and `triggerImmediateDataAnnounce`. Verify DATA peers connect and QDN transfers still work.

Step 1 alone is revertable; step 2 is where the behaviour changes land.

---

## 9. Register of intended behaviour changes

Everything here is deliberate. Anything else that changes behaviour is a regression.

| # | Change | Phase | Why | How to see it |
|---|---|---|---|---|
| 1 | `shutdown()` no longer NPEs on a null incoming link | 4 | §5.1 | shutdown log reaches "shutdown of Reticulum complete" |
| 2 | Snapshot rebuild atomic on remove | 4 | §5.2 — a peer could be live but invisible to every consumer | not directly observable; narrow race |
| 3 | Dynamic gateway dial runs off the announce thread | 7 | §5.4 — a slow gateway stalled announce processing | "Dynamically adding announced backbone gateway" now logged from `RNS-GatewayDial` |
| 4 | `maybeAnnounce` fires on `count < min`, not `count <= min` | 10 | announcing after the target is met is pointless traffic | one fewer announce per cycle at steady state |
| 5 | **DATA gains announce/reconnect watchdogs** (60 s / 45 s) | 10 | §3 — a wedged DATA announce currently stops DATA announces for the process lifetime | "announce task running for Ns — interrupting stuck task" now appears with a DATA prefix |
| 6 | **DATA gains circuit-breaker participation** | 10 | ditto — DATA never triggered `forceReconnect()` | backbone force-reconnect can now be triggered by DATA stalls |
| 7 | **DATA gains the 1-outgoing-link-per-cycle throttle** | 10 | DATA could create N simultaneous links and flood `jobsLock` | at DATA 0-peers, one "proactively connecting" line per 15 s cycle instead of N |
| 8 | **DATA skips peers already ACTIVE as incoming** | 10 | avoids duplicate links and doubled Channel teardown rate | fewer duplicate DATA peers in `/peers/reticulum` |
| 9 | Loop tick 10 ms → 50 ms | 11 | ~200 peer-list traversals/s at idle for ≤ 10 ms of latency | idle CPU of `rnsMesh-*` threads drops |
| 10 | Per-peer reconnect INFO → DEBUG | 11 | ~100 INFO lines per 15 s cycle with 50 known peers (~24 k/h) | log volume |
| 11 | `isUnreachable` returns `boolean` | 9 | §5.7 | none |
| 15 | `ReconnectPolicy.evictOlderThan(24 h)` per reconnect cycle | 10 | §5.5 — the failure maps only ever grew | a peer quiet for a day restarts from the 60 s base window |
| 16 | The reconnect pass tests "already tracked" against the live registry, not a list captured before the task was submitted | 10 | a peer added in between was dialled and then deduped by `addLinked` | one fewer wasted LINKREQUEST per race |
| 17 | Announce/reconnect executor threads renamed `RNS-<aspect>-Announce` / `-Reconnect` | 10 | one naming rule for two instances | `jcmd Thread.print`; `RNS-` prefix greps still match |
| 18 | `shutdown()` stops each loop thread **and its executors** before peer teardown, not after | 10 | a reconnect task must not create links while shutdown closes them | shutdown log ordering |
| 19 | `QAnnounceHandler`'s per-peer "peer exists" / "peer link" lines INFO → DEBUG | 11 | that loop runs per received announce, and every peer announces every ~30 s | log volume; "added new ReticulumPeer" stays at INFO |
| 12 | Zero-caller public methods removed | 1 | §3.3 | none in-tree; note for any out-of-tree consumer |
| 13 | `shutdown()` tolerates a mesh that never started | 4 | `start()` can now return early, and Controller calls `shutdown()` unconditionally — without this the guard in §6.4 would just move the NPE | "Reticulum mesh was not started — closing worker threads only" |
| 14 | Snapshot fields are `volatile` | 3 | written by mutators, read by every consumer thread; the `@Data` getter provided no barrier | none observable |

**Runtime verification.** A node was run at the phase-5 state (`9e9d8737`) — mesh forms, peers connect, Reticulum working normally. Phases 6–7 changed startup ordering (the peer stores are built in `start()`, not the constructor) and the gateway-dial threading, so **re-run a node before phase 8** and check: known-peer hashes still load at startup ("Loaded N known BASE peer hashes"), `/peers/reticulum` fills as before, and any dynamic gateway add now logs from the `RNS-GatewayDial` thread.

**Test placement.** The Reticulum unit tests live in `org.qortal.network.reticulum` under `src/test` (`RNSAnnounceCodecTest`, `RNSPeerFactoryScanTest`, `RNSPeerRegistryTest`, `RNSPeerPrunerTest`) — `RNSPeerRegistry` and `RNSPeerPruner` are package-private *on purpose*, since that visibility is what enforces "only RNS mutates the peer lists", and a same-package test is the only way to reach them without making the classes public. The repo already has this pattern (`src/test/java/org/qortal/controller`). `RNSNetworkTest`, which is an integration-style test rather than a unit test, stays in `org.qortal.test.network`.

**Testability fix, phase 8.** `ReticulumPeer.APP_NAME` (a `static final` reading `Settings`) forced the settings file, `BlockChain` and the crypto stack to load during class initialisation — mocking the class failed with `NoClassDefFoundError: NullAccount`, root-caused to `RIPEMD160 message digest not available`. It is now `appName()`, resolved on use. Only `initPeerLink()` ever read it. `RNS` has the same pattern in its own `APP_NAME`; harmless today (nothing mocks `RNS`) but worth the same treatment if it ever needs testing.

**Deviation, phase 7.** `@Synchronized` on `receivedAnnounce` is **kept**, not removed as §7.3 anticipated. What the analysis objected to was holding it across a TCP connect; that is fixed by moving the dial to an executor. The lock itself is cheap, and dropping it would let two announces for the same aspect race the `activePeerCount < peerLimit` check — harmless (`addLinkedPeer` dedups atomically) but a real concurrency change with no upside. Its limits are now documented at the annotation.

**Notes, phase 11.** Item 10's scope was "per-peer reconnect"; item 19 was added for `QAnnounceHandler`, which is the same class of problem (per-announce rather than per-cycle) and was the largest single INFO source left in `RNS.java` on a busy mesh. `hopsTo()` moved inside an `isDebugEnabled()` guard — it fed nothing but that log line. Deliberately left at INFO: the per-cycle interface online status (one or two lines per 15 s, and the first thing you read when the mesh is down), the announce timing lines that §11.2 checks, and everything in `RNSPeerPruner` — a peer removal is an event, not a poll.

**Notes, phase 10.** Landed as the two commits §8.4 asks for: `6bf85e93` wires BASE only and leaves `runDataLoop` untouched (revertable, and the state to run a node at), `b29ea96d` wires DATA and deletes the duplicate.

Deviations from §8.4's sketch:

- **`aspectName` is not a constructor parameter.** Nothing in the runner needs it — `registry.activeIncomingHashes(aspect)` derives it itself. Two parameters were added instead: `logInterfaceStatus` (interface state is transport-wide, so only BASE logs it — otherwise the line rate simply doubles) and `threadPriority` (the runner builds its own executors, so it needs what the `RNS` constructor used to read from `Settings`).
- **`peerFactory` is one method, not two.** `createLinkedPeerFromIdentity` takes the aspect; `new ReticulumPeer(dhash)` was already `new ReticulumPeer(dhash, BASE)`.
- **§8.4's `announce()` snippet contradicts §9 item 4** — it shows `if (count > minDesiredPeers) return;`, which is the old `<=` behaviour. §9 is authoritative per rule 3, so the code is `if (count >= minDesiredPeers) return;`. Worth a look on a live node: at exactly the peer target a node now stops announcing, which is the intended traffic saving but also makes it marginally less discoverable.
- **The watchdog helper takes `nowMs`** as well as the four planned parameters, and the "is a task already running" test is a `compareAndSet(0, nowMs)` rather than a read-then-write — the loop thread is the only writer, so this is defensive rather than a fix.

`RNS.java` is 854 L, not the ~250 L of §1. What remains is the facade proper (lifecycle, the singleton, the public API), plus `QAnnounceHandler` (~140 L), the two `*ClientConnected` callbacks, and the peer add/remove methods whose bodies are mostly the §10 comments. Splitting those out was never in the phase table; §13's "≤ 300 lines" is not met and would need a phase 12 to reach.

**Notes, phase 9.** Two behaviour-neutral simplifications beyond the plan: the before/after census reads `registry.activeIncoming().size()` instead of `incoming.size() - nonActiveIncoming().size()` (exact complements — one traversal, not two), and the silent-peer pass iterates `registry.activeIncoming()` instead of filtering `incoming()` by `status == ACTIVE` inline. Log text is unchanged. `RNS.getNonActiveIncomingPeers()` is deleted: after the extraction it had zero callers, and `registry.nonActiveIncoming()` is the accessor. `java.time.Duration` and the `LinkStatus.PENDING` static import leave `RNS.java` with the prune code — nothing else in the facade used either.

**Running notes (phases 1–5, as landed).** `mvn test` needs network access the first time — surefire's `junit-platform` provider is not in the local repo, and `-o` fails on it. Tests run with
`mvn -DskipTests -DskipJUnitTests=false -Dtest=RNSAnnounceCodecTest test` (the pom wires `<skipTests>` to the `skipJUnitTests` property, so `-DskipTests` alone does not control it).
`RNSAnnounceCodec.parseVersionToLong` uses `Peer.VERSION_PATTERN` rather than `ReticulumPeer.VERSION_PATTERN` — same constant, inherited from the interface, but it keeps the codec free of any peer class.

Items 5–8 are the point of the whole exercise: the DATA path is silently missing robustness fixes that were applied to BASE, and unifying the loops is the only way to stop that drift recurring.

---

## 10. Comments that must survive verbatim

These encode operational knowledge that is not re-derivable from the code. Each one moves with its code; none may be summarised away.

| Comment (current line) | New home |
|---|---|
| Dedicated single-thread executors / `jobsLock` busy-wait rationale (215–221) | `RNSAspectRunner` field block |
| `PENDING_FAILURE_BACKOFF_MS` / capped-exponential rationale (148–164) | `ReconnectPolicy` |
| Hybrid reconnect strategy: `createLinkedPeerFromIdentity` vs `requestPath` (738–756) | `RNSAspectRunner.reconnectTick` |
| Why the 1-link-per-cycle throttle exists (703–706) | `RNSAspectRunner.reconnectTick` |
| Why `activeIncomingBaseHashes` is precomputed (708–711) | `RNSPeerRegistry.activeIncomingHashes` |
| Why `createLinkedPeerFromIdentity` must **not** call `getOrInitPeerLink()` (1698–1703) | wherever that factory lands |
| Why `removeLinkedPeer` deliberately does **not** close `peerLink` (1948–1951) | `RNS.removeLinkedPeer` |
| Why `makePeerUnavailable()` runs with no RNS lock held (1954–1961) | `RNS.removeLinkedPeer` |
| `closeIfActive` ACTIVE-only + ABBA-inversion rationale (2078–2088) | `RNS.closeIfActive` |
| Why PENDING links must not be torn down (`expirePath` cull cascade) (2254–2262) | `RNSPeerPruner.pruneInitiatorPeers` |
| Watchdog-thread leak: 16,642 threads / RSS 34.8 G (2215–2222) | `RNSPeerPruner.pruneInitiatorPeers` |
| Why `announce()`/`requestPath()` are not called from `prunePeers` (2330–2333) | `RNSPeerPruner.prune` |
| Why `getAspectFilter()` returns null (1565–1569) | `QAnnounceHandler` |
| `createPeerBuffer` vs `getOrInitPeerBuffer` on the broadcast path (1154–1155) | `baseClientConnected` |
| `exitHandler()` timeout / zombie-channel rationale (1093–1095) | `RNS.shutdown` |
| Config truncation fix: delete before write (542–547) | `RNSConfigWriter` |
| `LINK_INBOUND_TIMEOUT_MS` ≈ 2× library KEEPALIVE (240–247) | `RNSPeerPruner` |

Check after each extraction phase:

```bash
git show HEAD --stat
git diff HEAD~1 -- '*.java' | grep '^-' | grep -E '^\-\s*(//|\*)' | grep -vE '^\-\s*//\s*$'
```

Every deleted comment line in that output must be accounted for: either it moved (find it in the `+` side) or it belonged to code deleted in phase 1.

---

## 11. Verification

### 11.1 Per-phase, mechanical

```bash
mvn -q -DskipTests compile                       # every phase
mvn -q -DskipTests test-compile                  # catches test-side breakage from phase 1 and 2
mvn -q -Dtest='RNSAnnounceCodecTest' test        # phase 5 onwards
```

Post-phase greps that must return nothing:

```bash
# phase 3: no Lombok setters left on RNS
grep -rn "\.set\(Reticulum\|ServerIdentity\|LinkedPeers\|IncomingPeers\|ShuttingDown\)(" src/main --include='*.java'
# phase 4: no live-list iteration
grep -n "getIncomingPeers()\|getLinkedPeers()" src/main/java/org/qortal/network/reticulum/RNS.java
# phase 1: no fully-qualified names where an import exists (§5.8)
grep -n "java\.util\.concurrent\.\|java\.time\.Duration\|io\.reticulum\.interfaces\." \
     src/main/java/org/qortal/network/reticulum/RNS.java
# phase 10: no BASE/DATA duplicated field pairs left
grep -nE "dataAnnounce|dataReconnect|lastDataLoop|pendingDataLink" \
     src/main/java/org/qortal/network/reticulum/RNS.java
```

### 11.2 Runtime, after phase 2 and after each of 7/8/10

Compiling does not prove the reflection-based `PeerFactory` registration still works, nor that the mesh forms. Run a node and check:

1. `Reticulum config exists, skipping.` / `reticulum instance created` — construction path intact.
2. `RNS mesh started, baseDestination: <hash>` — `start()` completed.
3. `GET /peers/reticulum` returns peers with both `BASE` and `DATA` aspects, initiator and incoming.
4. Over ≥ 1 h: no `ConcurrentModificationException`, no NPE in `shutdown`, `Removing PENDING link stuck for Ns` appears at a similar rate to the baseline (not more).
5. Thread count stable: `jcmd <pid> Thread.print | grep -c "RNS-\|rnsMesh-"` — after phase 10 expect *fewer* threads (four aspect executors are now created by the runners, not by the RNS constructor, and the count is unchanged at 4; the watchdog-cancel path no longer spawns replacements).
6. Clean shutdown: `stop.sh` reaches `shutdown of Reticulum complete` within ~15 s, JVM exits.

Capture a baseline of 3–6 on the current build **before** starting phase 1, so the comparison is real.

---

## 12. Deviations from the analysis

| Analysis says | Plan does | Why |
|---|---|---|
| §8.1 "make `AnnounceInfo` a package-private record" | final class with final fields | `pom.xml` sets `<release>11</release>`; records need 16+ |
| §8.6 extract `RNSMessaging` (~120 L) | delete instead | `onPingMessage`, `buildHeightOrChainTipInfo`, `peerMisbehaved`, `getMessageMagic`'s external form, `findPeerBy*`, `getOurNodeId` have **zero callers** — all live callers use the `Network`/`NetworkData` equivalents. Only `broadcast` (Network.java:2753) survives, and it is 15 lines; it stays on the facade |
| §9 phase 1 "replace `@Data`" first | dead-code deletion and the package move come first | the move produces a cleaner rename diff when file content is not simultaneously edited, and deleting first shrinks what has to move |
| §5.6 "constructor swallows failure" | guard in `start()`, constructor still non-throwing | throwing from the constructor turns into an `ExceptionInInitializerError` inside `SingletonContainer`, which is strictly worse than a mesh that stays down with `isMeshStarted() == false` |
| §10 "put all reticulum java files in the folder" | includes `RNSCommon.java`, at the cost of 5 import updates in the IP path | `RNSCommon.PeerMetaType` is shared with `Peer`/`IPPeer`/`Network`/`NetworkData`/`PeerData`; the alternative (leave it in place) is noted in §4 |

---

## 13. Definition of done

- `src/main/java/org/qortal/network/reticulum/RNS.java` ≤ 300 lines, no `@Data`, no setters, 18-member public surface.
- No `runDataLoop`; one `RNSAspectRunner` class instantiated twice; no `data*` mirror fields.
- `RNSAnnounceCodecTest` green, ≥ 10 cases, covering legacy QGW1 and truncation.
- Every comment in §10 present in the new tree (grep-verified).
- A node runs ≥ 24 h with BASE and DATA peers connected, stable thread count, no CME/NPE, and shuts down cleanly.
- §9 is the complete diff in observable behaviour; nothing outside it changed.