package org.qortal.network.reticulum;

import io.reticulum.Reticulum;
import io.reticulum.Transport;
import io.reticulum.interfaces.ConnectionInterface;
import io.reticulum.destination.Destination;
import io.reticulum.destination.DestinationType;
import io.reticulum.destination.Direction;
import io.reticulum.destination.ProofStrategy;
import io.reticulum.identity.Identity;
import io.reticulum.identity.IdentityKnownDestination;
import io.reticulum.link.Link;
import io.reticulum.transport.AnnounceHandler;
import static io.reticulum.link.LinkStatus.ACTIVE;
import static io.reticulum.link.LinkStatus.CLOSED;
import static io.reticulum.link.LinkStatus.PENDING;
import static io.reticulum.utils.DestinationUtils.hashFromNameAndIdentity;
import lombok.Getter;
import lombok.Synchronized;

import org.qortal.network.Peer;
import org.qortal.network.message.*;
import org.qortal.repository.DataException;
import org.qortal.settings.Settings;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.nonNull;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.time.Duration;
import java.time.Instant;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.commons.codec.binary.Hex.decodeHex;
import static org.apache.commons.codec.binary.Hex.encodeHexString;
import org.qortal.utils.ExecuteProduceConsume;
import org.qortal.utils.NTP;
import org.qortal.utils.NamedThreadFactory;
import org.qortal.data.network.PeerData;
import org.qortal.controller.Controller;

// logging
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class RNS {

    private Reticulum reticulum;
    static final String APP_NAME = Settings.getInstance().isTestNet() ? RNSCommon.TESTNET_APP_NAME: RNSCommon.MAINNET_APP_NAME;
    static final Integer TARGET_PORT = Settings.getInstance().isTestNet() ? RNSCommon.TESTNET_IF_TCP_PORT: RNSCommon.MAINNET_IF_TCP_PORT;
    static final String defaultConfigPath = Settings.getInstance().isTestNet() ? RNSCommon.defaultRNSConfigPathTestnet: RNSCommon.defaultRNSConfigPath;
    static final String CORE_ASPECT = "qortal.core";
    static final String QDN_ASPECT  = "qortal.qdn";
    private final int MIN_DESIRED_CORE_PEERS = Settings.getInstance().getReticulumMinDesiredCorePeers();
    private final int MIN_DESIRED_DATA_PEERS = Settings.getInstance().getReticulumMinDesiredDataPeers();

    // Only the accessors below are exported. This class used to carry Lombok @Data, which
    // generated ~100 public accessors (including setters for every field and getters handing out
    // the live peer lists) to serve a handful of real call sites. Everything else is internal.
    @Getter private Identity serverIdentity;
    @Getter private Destination baseDestination;
    @Getter private Destination dataDestination;
    @Getter private volatile boolean isShuttingDown = false;
    private volatile boolean meshStarted = false;   // exported via isMeshStarted()

    // Persisted destination hashes of peers we have talked to, one store per aspect, so a restart
    // reconnects immediately instead of waiting for announces. See KnownPeerStore for why the
    // confirmed and loaded sets are kept apart. Created in start(), once the storage path is known.
    private static final String KNOWN_PEERS_FILE = "known_peer_hashes.txt";
    private static final String KNOWN_DATA_PEERS_FILE = "known_data_peer_hashes.txt";
    private KnownPeerStore basePeerStore;
    private KnownPeerStore dataPeerStore;

    // Tracks hashes of peers whose PENDING links were pruned as stuck (>60 s without establishing).
    // When a peer is unreachable, createLinkedPeerFromIdentity() creates a PENDING link that the
    // Reticulum library times out at ~75 s → expirePath() → 60-120 s cull → cascade.
    // After a stuck-PENDING failure or immediate send failure, we back off to requestPath() for
    // PENDING_FAILURE_BACKOFF_MS so the backbone can provide a fresh announce path.
    private final ConcurrentHashMap<String, Long> pendingLinkFailureMs =
            new ConcurrentHashMap<>();
    private static final long PENDING_FAILURE_BACKOFF_MS = 60_000L; // base backoff (first failure); 60s
    // Consecutive PENDING/link failures per peer hash (BASE and DATA hashes are distinct, so one map
    // serves both). Drives CAPPED EXPONENTIAL backoff: a permanently-unreachable peer (e.g. a mis-
    // configured/partitioned mesh) would otherwise be retried every ~120s forever, each retry firing
    // a PENDING link → expirePath() cull cascade → sustained reconnect-thread CPU. Backoff doubles per
    // failure up to MAX_PENDING_FAILURE_BACKOFF_MS, so stale peers become effectively dormant. Reset on
    // a successful ACTIVE connection (confirmPeerHash) so transient outages aren't penalised long-term.
    private final ConcurrentHashMap<String, Integer> pendingFailureCount =
            new ConcurrentHashMap<>();
    private static final long MAX_PENDING_FAILURE_BACKOFF_MS = 30 * 60_000L; // 30 min cap

    /**
     * Maintain two lists for each subset of peers
     *  => a synchronizedList, modified when peers are added/removed
     *  => an immutable List, automatically rebuild to mirror synchronizedList, served to consumers
     *  linkedPeers are "initiators" (containing initiator reticulum Link), actively doing work.
     *  incomimgPeers are "non-initiators", the passive end of bidirectional Reticulum Buffers.
     */
    private final List<ReticulumPeer> linkedPeers = Collections.synchronizedList(new ArrayList<>());
    @Getter private volatile List<ReticulumPeer> immutableLinkedPeers = Collections.emptyList();
    private final List<ReticulumPeer> incomingPeers = Collections.synchronizedList(new ArrayList<>());
    @Getter private volatile List<ReticulumPeer> immutableIncomingPeers = Collections.emptyList();

    // Gateway announce (reticulumAnnounceGateway): advertise-host resolution and dialling of
    // peer-announced gateways live in RNSGatewayManager; the announce payload that carries them
    // (QAN1 container, legacy QGW1 fallback) lives in RNSAnnounceCodec.
    private final RNSGatewayManager gatewayManager;

    /** Announced version keyed by identity hash (hex). Lets incoming peers (which have no announce
     *  at construction) resolve their version once they identify. Bounded LRU to avoid unbounded
     *  growth from mesh-wide announces. */
    private final Map<String, String> announcedVersions = Collections.synchronizedMap(
            new LinkedHashMap<String, String>(64, 0.75f, true) {
                @Override protected boolean removeEldestEntry(Map.Entry<String, String> e) { return size() > 512; }
            });

    /** Produces Connect tasks for the baseDestination and submits to worker pool. */
    private Thread rnsBaseThread;
    private Thread rnsDataThread;
    private ExecutorService rnsWorkerPool;
    // Dedicated single-thread executors for announce and reconnect (BASE and DATA).
    // Root cause of prior failures: Transport.outbound() busy-waits on jobsLock (non-interruptible).
    // A full table cull triggered by link drops holds jobsLock for 30-60s. With a shared pool,
    // each watchdog reset spawns a new thread, creating 20+ threads all spinning on jobsLock
    // simultaneously — massively worsening contention and making the cull take even longer.
    // Solution: one dedicated thread per operation (bounded queue=1). At most 2 threads ever
    // spin on jobsLock; tasks queue up naturally and complete when the cull finishes.
    private ExecutorService announceExecutor;
    private ExecutorService reconnectExecutor;
    private ExecutorService dataAnnounceExecutor;
    private ExecutorService dataReconnectExecutor;
    private static final long NETWORK_EPC_KEEPALIVE = 5L; // 1 second

    // replicating a feature from Network.class needed in for base Message.java,
    // just in case the classic TCP/IP Networking is turned off.
    private static final byte[] MAINNET_MESSAGE_MAGIC = new byte[]{0x51, 0x4f, 0x52, 0x54}; // QORT
    private static final byte[] TESTNET_MESSAGE_MAGIC = new byte[]{0x71, 0x6f, 0x72, 0x54}; // qort
    /**
     * How long a Link may go with no inbound activity before we treat it as unreachable. Liveness
     * now comes from the Reticulum Link's native keepalive via its (library-fixed) lastInbound,
     * which is refreshed on real traffic AND on keepalive round-trips (every ~360s, the library
     * KEEPALIVE, when idle). Allow ~2x that so an idle-but-alive link riding on keepalives alone is
     * not culled. Replaces the old app-level ping + 165s lastAccessTimestamp staleness.
     */
    private static final long LINK_INBOUND_TIMEOUT_MS = 2 * 360 * 1000L; // ms (~2x library KEEPALIVE)
    /**
     * How often runBaseLoop() triggers maybeAnnounce() and path recovery, independent
     * of prunePeers(). This ensures announces fire even when the Controller scheduler is
     * slow/blocked (e.g., prunePeers() waiting on a lock inside the Reticulum library).
     */
    private static final long BASE_LOOP_ANNOUNCE_INTERVAL_MS = 30_000L; // 30 seconds
    private static final long BASE_LOOP_RECONNECT_INTERVAL_MS = 15_000L; // reconnect independently of announce
    private static final long ANNOUNCE_TASK_TIMEOUT_MS = 60_000L; // watchdog: reset stuck announce after 60s
    private static final long RECONNECT_TASK_TIMEOUT_MS = 45_000L; // watchdog: reset stuck reconnect after 45s
    private volatile long lastBaseLoopAnnounceMs = 0;
    private volatile long lastBaseLoopReconnectMs = 0;
    // Timestamp-based guards: 0 = no task running; non-zero = task started at that ms.
    // Timestamps (rather than booleans) allow a watchdog to force-reset after the timeout.
    // createLinkedPeerFromIdentity() and requestPath() call Reticulum transport code that can
    // acquire internal locks and block when the backbone degrades — both must run in the pool,
    // never inline on the runBaseLoop thread.
    private volatile long announceTaskStartedMs = 0L;
    private volatile long reconnectTaskStartedMs = 0L;
    private volatile Future<?> announceTaskFuture = null;
    private volatile Future<?> reconnectTaskFuture = null;
    // Circuit breaker: when both announce and reconnect tasks keep timing out consecutively,
    // the backbone TCP connection is likely in a bad state. Force-close it to trigger the
    // library's built-in auto-reconnect rather than spinning on a stuck jobsLock forever.
    private volatile int consecutiveStuckTasks = 0;
    private static final int BACKBONE_FORCE_RECONNECT_THRESHOLD = 2;

    // DATA loop timing — mirrors BASE, separate so DATA and BASE don't interfere
    private static final long DATA_LOOP_ANNOUNCE_INTERVAL_MS  = 30_000L;
    private static final long DATA_LOOP_RECONNECT_INTERVAL_MS = 15_000L;
    private volatile long lastDataLoopAnnounceMs  = 0;
    private volatile long lastDataLoopReconnectMs = 0;
    private volatile long dataAnnounceTaskStartedMs  = 0L;
    private volatile long dataReconnectTaskStartedMs = 0L;
    private volatile Future<?> dataAnnounceTaskFuture  = null;
    private volatile Future<?> dataReconnectTaskFuture = null;

    private final ConcurrentHashMap<String, Long> pendingDataLinkFailureMs =
            new ConcurrentHashMap<>();

    /**
     * Record a PENDING/link-establishment failure for a peer: stamp the failure time in the
     * aspect-specific time map and increment the shared failure counter (for exponential backoff).
     */
    private void recordPendingFailure(String hashHex,
            ConcurrentHashMap<String, Long> timeMap) {
        timeMap.put(hashHex, System.currentTimeMillis());
        pendingFailureCount.merge(hashHex, 1, Integer::sum);
    }

    /**
     * Capped exponential backoff window for a peer: {@code 60s, 120s, 240s, … , 30min}. The window
     * grows with the consecutive-failure count so peers that never connect are retried ever less
     * frequently (bounding PENDING-link creation and its expirePath cull cascade), while a
     * first/occasional failure still retries quickly.
     */
    private long pendingBackoffMs(String hashHex) {
        int count = pendingFailureCount.getOrDefault(hashHex, 0);
        if (count <= 1) return PENDING_FAILURE_BACKOFF_MS;
        int shift = Math.min(count - 1, 9); // guard against overflow; cap below clamps the value anyway
        long ms = PENDING_FAILURE_BACKOFF_MS << shift;
        return Math.min(ms, MAX_PENDING_FAILURE_BACKOFF_MS);
    }

    /** Clear failure/backoff state for a peer that has connected successfully. */
    private void clearPendingFailure(String hashHex) {
        pendingFailureCount.remove(hashHex);
        pendingLinkFailureMs.remove(hashHex);
        pendingDataLinkFailureMs.remove(hashHex);
    }

    /** Called by ReticulumPeer.linkClosed() to kick the announce/path-recovery cycle soon.
     *  Uses a 5s delay rather than 0 to avoid tight reconnect loops when links close rapidly
     *  (e.g., Channel "retry count exceeded" tears down a link, immediate re-announce creates
     *  a new link, new link also fails → rapid churn). */
    public void triggerImmediateAnnounce() {
        this.lastBaseLoopAnnounceMs = System.currentTimeMillis() - BASE_LOOP_ANNOUNCE_INTERVAL_MS + 5_000L;
    }

    /**
     * Called when a stuck task is interrupted. When the threshold is reached, force-closes
     * the backbone TCP channel so the library's built-in auto-reconnect fires, clearing any
     * jobsLock deadlock caused by a zombie-link cull cascade.
     */
    private void maybeForceBackboneReconnect() {
        if (consecutiveStuckTasks < BACKBONE_FORCE_RECONNECT_THRESHOLD) return;
        consecutiveStuckTasks = 0; // reset so we don't spam per-interval
        log.warn("runBaseLoop: {} consecutive stuck tasks — forcing backbone TCP reconnect to clear deadlock",
                BACKBONE_FORCE_RECONNECT_THRESHOLD);
        gatewayManager.forceBackboneReconnect();
    }

    // Constructor
    public RNS () {
        log.info("RNS constructor");
        try {
            log.info("creating config in {}", defaultConfigPath);
            RNSConfigWriter.ensureConfig(defaultConfigPath, APP_NAME, TARGET_PORT);
            reticulum = new Reticulum(defaultConfigPath);
            var identitiesPath = reticulum.getStoragePath().resolve("identities");
            if (Files.notExists(identitiesPath)) {
                Files.createDirectories(identitiesPath);
            }
        } catch (IOException e) {
            log.error("unable to create Reticulum network", e);
        }
        log.info("reticulum instance created");
        log.debug("reticulum instance created: {}", reticulum);
        var rnsThreadPriority = Settings.getInstance().getNetworkThreadPriority(); // default: 7
        this.gatewayManager = new RNSGatewayManager(APP_NAME, TARGET_PORT, rnsThreadPriority);
        this.rnsWorkerPool = new ThreadPoolExecutor(
                3, Settings.getInstance().getReticulumMaxNetworkThreadPoolSize(),
                NETWORK_EPC_KEEPALIVE, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                new NamedThreadFactory("RNS-Worker", rnsThreadPriority));
        // Bounded queue(1): at most one task running + one queued. A rejected submission just
        // means the next interval will retry — no unbounded thread growth.
        this.announceExecutor = new ThreadPoolExecutor(1, 1,
                NETWORK_EPC_KEEPALIVE, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(1),
                new NamedThreadFactory("RNS-Announce", rnsThreadPriority));
        this.reconnectExecutor = new ThreadPoolExecutor(1, 1,
                NETWORK_EPC_KEEPALIVE, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(1),
                new NamedThreadFactory("RNS-Reconnect", rnsThreadPriority));
        this.dataAnnounceExecutor = new ThreadPoolExecutor(1, 1,
                NETWORK_EPC_KEEPALIVE, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(1),
                new NamedThreadFactory("RNS-DataAnnounce", rnsThreadPriority));
        this.dataReconnectExecutor = new ThreadPoolExecutor(1, 1,
                NETWORK_EPC_KEEPALIVE, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(1),
                new NamedThreadFactory("RNS-DataReconnect", rnsThreadPriority));
    }

    // Note: potentially create persistent serverIdentity (utility rnid) and load it from file
    public void start() {
        // The constructor logs and continues when the Reticulum stack can't be built, so the
        // singleton is published half-built. Dereferencing reticulum here would then NPE inside
        // Network's startup. Refuse instead: meshStarted stays false and every consumer already
        // guards on isMeshStarted(), so the node runs without the mesh rather than failing.
        if (reticulum == null) {
            log.error("Reticulum stack unavailable (see construction error above) — mesh will not start");
            return;
        }

        // create identity either from file or new (creating new keys)
        var serverIdentityPath = reticulum.getStoragePath().resolve("identities/"+APP_NAME);
        if (Files.isReadable(serverIdentityPath)) {
            serverIdentity = Identity.fromFile(serverIdentityPath);
            log.info("server identity loaded from file {}", serverIdentityPath);
        } else {
            serverIdentity = new Identity();
            log.info("APP_NAME: {}, storage path: {}", APP_NAME, serverIdentityPath);
            log.info("new server identity created dynamically.");
            // save it back to file by default for next start (possibly add setting to override)
            try {
                Files.write(serverIdentityPath, serverIdentity.getPrivateKey(), CREATE, WRITE);
                log.info("serverIdentity written back to file");
            } catch (IOException e) {
                log.error("Error while saving serverIdentity to {}", serverIdentityPath, e);
            }
        }
        log.debug("Server Identity: {}", serverIdentity.toString());

        // show the ifac_size of the configured interfaces (debug code)
        for (ConnectionInterface i: Transport.getInstance().getInterfaces() ) {
            log.debug("interface {}, length: {}", i.getInterfaceName(), i.getIfacSize());
        }

        baseDestination = new Destination(
            serverIdentity,
            Direction.IN,
            DestinationType.SINGLE,
            APP_NAME,
            "core"
        );
        log.info("Destination {} {} running", encodeHexString(baseDestination.getHash()), baseDestination.getName());
        dataDestination = new Destination(
            serverIdentity,
            Direction.IN,
            DestinationType.SINGLE,
            APP_NAME,
            "qdn"
        );
        log.info("Destination {} {} running", encodeHexString(dataDestination.getHash()), dataDestination.getName());
   
        baseDestination.setProofStrategy(ProofStrategy.PROVE_ALL);
        baseDestination.setAcceptLinkRequests(true);
        dataDestination.setProofStrategy(ProofStrategy.PROVE_ALL);
        dataDestination.setAcceptLinkRequests(true);

        baseDestination.setLinkEstablishedCallback(this::baseClientConnected);
        dataDestination.setLinkEstablishedCallback(this::dataClientConnected);
        Transport.getInstance().registerAnnounceHandler(new QAnnounceHandler(CORE_ASPECT));
        Transport.getInstance().registerAnnounceHandler(new QAnnounceHandler(QDN_ASPECT));
        log.debug("announceHandlers: {}", Transport.getInstance().getAnnounceHandlers());
        // Load peer hashes persisted from previous run so we can call requestPath() fast on restart.
        this.basePeerStore = new KnownPeerStore(reticulum.getStoragePath(), KNOWN_PEERS_FILE, "BASE");
        this.dataPeerStore = new KnownPeerStore(reticulum.getStoragePath(), KNOWN_DATA_PEERS_FILE, "DATA");
        this.basePeerStore.load();
        this.dataPeerStore.load();
        // do a first announce (across all configured interfaces)
        byte[] initialAppData = buildAnnounceAppData();
        baseDestination.announce(initialAppData);
        log.info("Sent initial announce from {} ({})", encodeHexString(baseDestination.getHash()), baseDestination.getName());
        dataDestination.announce(initialAppData);
        log.info("Sent initial announce from {} ({})", encodeHexString(dataDestination.getHash()), dataDestination.getName());
        // Seed loop announce timers. On restart (non-empty loaded hashes) fire path requests
        // at t=15s; on first-ever start use the full 30s window.
        this.lastBaseLoopAnnounceMs = this.basePeerStore.hasLoadedHashes()
                ? System.currentTimeMillis() - BASE_LOOP_ANNOUNCE_INTERVAL_MS + 15_000L
                : System.currentTimeMillis();
        this.lastDataLoopAnnounceMs = this.dataPeerStore.hasLoadedHashes()
                ? System.currentTimeMillis() - DATA_LOOP_ANNOUNCE_INTERVAL_MS + 15_000L
                : System.currentTimeMillis();

        // Start up "main" threads, one per destination / peer aspect.
        this.rnsBaseThread = new Thread(this::runBaseLoop, "rnsMesh-BASE");
        this.rnsBaseThread.setDaemon(true);
        this.rnsBaseThread.start();
        this.rnsDataThread = new Thread(this::runDataLoop, "rnsMesh-DATA");
        this.rnsDataThread.setDaemon(true);
        this.rnsDataThread.start();

        this.meshStarted = true;
        log.info("RNS mesh started, baseDestination: {}", encodeHexString(baseDestination.getHash()));
    }

    public boolean isMeshStarted() {
        return meshStarted;
    }

    // "main" loop for baseDestination (chain tasks)
    private void runBaseLoop() {
        while (!isShuttingDown && !Thread.currentThread().isInterrupted()) {
            try {
                // Drain messages from both initiator peers (linkedPeers) and
                // non-initiator/incoming peers (incomingPeers) so that requests
                // received by either side are processed.
                final List<ReticulumPeer> peersThisRound = Stream.concat(
                        this.getActiveImmutableLinkedPeers().stream()
                                .filter(p -> p.getPeerAspect() == RNSCommon.PeerAspect.BASE),
                        this.getImmutableIncomingPeers().stream()
                                .filter(p -> p.getPeerAspect() == RNSCommon.PeerAspect.BASE)
                                .filter(p -> {
                                    var pl = p.getPeerLink();
                                    return nonNull(pl) && pl.getStatus() == ACTIVE;
                                })
                ).collect(Collectors.toList());

                final Long now = NTP.getTime();
                for (ReticulumPeer peer : peersThisRound) {
                    ExecuteProduceConsume.Task task;
                    while ((task = peer.getMessageTask(Peer.NETWORK)) != null) {
                        final ExecuteProduceConsume.Task t = task;
                        try {
                            rnsWorkerPool.execute(() -> {
                                try {
                                    t.perform();
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                } catch (Exception e) {
                                    log.warn("Reticulum worker task threw: {}", e.getMessage(), e);
                                }
                            });
                        } catch (RejectedExecutionException e) {
                            log.warn("[{}] Reticulum worker pool rejected message task (pool full or shutting down)",
                                    peer.getPeerConnectionId());
                            break;
                        }
                    }

                    // Send keepalive ping if due (initiator peers only, every 55s)
                    ExecuteProduceConsume.Task pingTask = peer.getPingTask(now);
                    if (pingTask != null) {
                        final ExecuteProduceConsume.Task pt = pingTask;
                        try {
                            rnsWorkerPool.execute(() -> {
                                try {
                                    pt.perform();
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                } catch (Exception e) {
                                    log.warn("Reticulum ping task threw: {}", e.getMessage(), e);
                                }
                            });
                        } catch (RejectedExecutionException e) {
                            log.warn("[{}] Reticulum worker pool rejected ping task", peer.getPeerConnectionId());
                        }
                    }
                }

                // Periodic announce — dedicated single-thread executor with bounded queue(1).
                // Transport.outbound() busy-waits on jobsLock (non-interruptibly); a table cull
                // triggered by link drops can hold jobsLock for 30-60s.  With a single thread,
                // at most 1 task spins on the lock at a time; a queued task runs as soon as the
                // running one completes.  Rejected = queue full = there is already one waiting.
                long nowMs = System.currentTimeMillis();
                if (nowMs - lastBaseLoopAnnounceMs >= BASE_LOOP_ANNOUNCE_INTERVAL_MS) {
                    lastBaseLoopAnnounceMs = nowMs;
                    long taskStart = announceTaskStartedMs;
                    if (taskStart != 0 && (nowMs - taskStart > ANNOUNCE_TASK_TIMEOUT_MS)) {
                        log.warn("runBaseLoop: announce task running for {}s — interrupting stuck task",
                                (nowMs - taskStart) / 1000);
                        Future<?> f = announceTaskFuture;
                        if (f != null && !f.isDone()) f.cancel(true);
                        ((ThreadPoolExecutor) announceExecutor).purge();
                        announceTaskStartedMs = 0L;
                        consecutiveStuckTasks++;
                        maybeForceBackboneReconnect();
                    }
                    if (announceTaskStartedMs == 0L) {
                        announceTaskStartedMs = nowMs;
                        try {
                            announceTaskFuture = announceExecutor.submit(() -> {
                                Thread.interrupted(); // clear any stale interrupt flag from prior cancel
                                try {
                                    maybeAnnounce(getBaseDestination(), RNSCommon.PeerAspect.BASE);
                                } catch (Exception e) {
                                    log.warn("Exception in base loop announce: {}", e.getMessage(), e);
                                } finally {
                                    // Reset counter only if watchdog didn't fire — watchdog sets
                                    // announceTaskStartedMs=0 before incrementing consecutiveStuckTasks,
                                    // so a non-zero value here means we completed without intervention.
                                    if (announceTaskStartedMs != 0L) {
                                        consecutiveStuckTasks = 0;
                                    }
                                    announceTaskStartedMs = 0L;
                                }
                            });
                        } catch (RejectedExecutionException e) {
                            announceTaskStartedMs = 0L;
                        }
                    }
                }

                // Periodic path recovery — submitted to rnsWorkerPool so that createLinkedPeerFromIdentity()
                // and requestPath() (which call Reticulum transport code that acquires internal locks and can
                // block on backbone I/O) do not freeze the runBaseLoop thread.
                if (nowMs - lastBaseLoopReconnectMs >= BASE_LOOP_RECONNECT_INTERVAL_MS) {
                    lastBaseLoopReconnectMs = nowMs;
                    long rTaskStart = reconnectTaskStartedMs;
                    if (rTaskStart != 0 && (nowMs - rTaskStart > RECONNECT_TASK_TIMEOUT_MS)) {
                        log.warn("runBaseLoop: reconnect task running for {}s — interrupting stuck task",
                                (nowMs - rTaskStart) / 1000);
                        Future<?> rf = reconnectTaskFuture;
                        if (rf != null && !rf.isDone()) rf.cancel(true);
                        ((ThreadPoolExecutor) reconnectExecutor).purge();
                        reconnectTaskStartedMs = 0L;
                        consecutiveStuckTasks++;
                        maybeForceBackboneReconnect();
                    }
                    if (reconnectTaskStartedMs == 0L) {
                        reconnectTaskStartedMs = nowMs;
                        final int activeLinked = getActiveImmutableLinkedPeers().size();
                        final List<ReticulumPeer> currentLinked = getImmutableLinkedPeers();
                        final Set<String> reconnectTargets = basePeerStore.reconnectTargets();
                        try {
                            reconnectTaskFuture = reconnectExecutor.submit(() -> {
                                Thread.interrupted(); // clear any stale interrupt flag from prior cancel
                                try {
                                    // Log interface online status for diagnostics
                                    for (ConnectionInterface iface : Transport.getInstance().getInterfaces()) {
                                        log.info("Interface '{}' online={}", iface.getInterfaceName(), iface.isOnline());
                                    }
                                    if (activeLinked < MIN_DESIRED_CORE_PEERS && !reconnectTargets.isEmpty()) {
                                        log.info("Active linked peers {} < desired {} (base loop); requesting paths to {} known peers",
                                                activeLinked, MIN_DESIRED_CORE_PEERS, reconnectTargets.size());
                                        // When fully disconnected, limit outgoing link creation to 1 per cycle.
                                        // Creating all peers simultaneously floods jobsLock (each new Link() sends
                                        // a LINKREQUEST via outbound(Packet)) and starves announce/reconnect tasks.
                                        // The PENDING-failure backoff naturally rotates through peers across cycles.
                                        int outgoingLinksCreated = 0;
                                        // Precompute the identity hashes of ACTIVE incoming BASE peers ONCE per cycle.
                                        // Previously this was recomputed per target via a stream + hashFromNameAndIdentity
                                        // (a SHA-256) over every incoming peer — O(targets × incoming) crypto hashing each
                                        // cycle. A set lookup makes the per-target check O(1).
                                        final Set<String> activeIncomingBaseHashes = new HashSet<>();
                                        for (ReticulumPeer ip : getImmutableIncomingPeers()) {
                                            Link ipl = ip.getPeerLink();
                                            Identity rid = ip.getServerIdentity();
                                            if (nonNull(ipl) && ipl.getStatus() == ACTIVE && rid != null) {
                                                activeIncomingBaseHashes.add(
                                                        encodeHexString(hashFromNameAndIdentity(CORE_ASPECT, rid)));
                                            }
                                        }
                                        for (String hashHex : reconnectTargets) {
                                            try {
                                                byte[] dhash = decodeHex(hashHex);
                                                // Skip peers already tracked (PENDING or ACTIVE) as initiator links
                                                boolean tracked = currentLinked.stream()
                                                        .anyMatch(p -> Arrays.equals(p.getDestinationHash(), dhash));
                                                if (tracked) continue;
                                                // Skip peers already ACTIVE as incoming — broadcast() covers them,
                                                // and creating a duplicate outgoing link doubles the Channel teardown
                                                // rate, driving more expirePath() culls and accumulating spurious
                                                // incoming connections on the remote end. (O(1) set lookup — see the
                                                // precomputed activeIncomingBaseHashes above.)
                                                if (activeIncomingBaseHashes.contains(hashHex)) continue;
                                                // hopsTo() is a ConcurrentHashMap.get() — no lock, always safe.
                                                int hops = Transport.getInstance().hopsTo(dhash);
                                                log.info("Path to {}: hops={}", hashHex,
                                                        hops == io.reticulum.constant.TransportConstant.PATHFINDER_M ? "unknown" : hops);
                                                // Hybrid reconnect strategy:
                                                //
                                                // createLinkedPeerFromIdentity() creates an outgoing link immediately
                                                // from the locally-cached identity. This is how initial connections form.
                                                // If the LINKREQUEST send fails (no route in pathTable), the link is
                                                // CLOSED immediately and we record pendingLinkFailureMs right there.
                                                // If the peer is reachable but slow, the RNS.java pruner removes the
                                                // PENDING link after 60s and records pendingLinkFailureMs.
                                                // Either way we back off to requestPath() for PENDING_FAILURE_BACKOFF_MS
                                                // so the backbone can provide a fresh path before we retry.
                                                //
                                                // requestPath() sends a single path-request packet (no PENDING link).
                                                // If the backbone responds with a fresh announce, QAnnounceHandler creates
                                                // the link. If the peer is unreachable nothing happens: no cull, no cascade.
                                                //
                                                // Strategy: use createLinkedPeerFromIdentity() for peers without a recent
                                                // PENDING failure; use requestPath() for peers in the backoff window.
                                                // When activeLinked==0, limit outgoing link creation to 1 per cycle to
                                                // avoid flooding jobsLock; requestPath breaks the 0/0 deadlock for others.
                                                long lastFailure = pendingLinkFailureMs.getOrDefault(hashHex, 0L);
                                                boolean recentlyFailed = (System.currentTimeMillis() - lastFailure) < pendingBackoffMs(hashHex);
                                                boolean outgoingSlotFree = activeLinked > 0 || outgoingLinksCreated == 0;
                                                Identity cachedIdentity = (!recentlyFailed && outgoingSlotFree)
                                                        ? IdentityKnownDestination.recall(dhash) : null;
                                                if (cachedIdentity != null) {
                                                    log.info("Proactively connecting to {} via cached identity", hashHex);
                                                    createLinkedPeerFromIdentity(dhash, cachedIdentity);
                                                    outgoingLinksCreated++;
                                                } else {
                                                    if (recentlyFailed) {
                                                        log.info("Backing off to requestPath for {} (recent PENDING failure)", hashHex);
                                                    } else if (!outgoingSlotFree) {
                                                        log.info("requestPath for {} (outgoing slot in use)", hashHex);
                                                    } else {
                                                        log.info("requestPath for {} (no cached identity)", hashHex);
                                                    }
                                                    Transport.getInstance().requestPath(dhash);
                                                }
                                            } catch (Exception e) {
                                                log.warn("Path request/reconnect failed for {}: {}", hashHex, e.getMessage());
                                            }
                                        }
                                    }
                                } catch (Exception e) {
                                    log.warn("Exception in base loop reconnect: {}", e.getMessage(), e);
                                } finally {
                                    // Reset counter only if watchdog didn't fire (same logic as announce task).
                                    if (reconnectTaskStartedMs != 0L) {
                                        consecutiveStuckTasks = 0;
                                    }
                                    reconnectTaskStartedMs = 0L;
                                }
                            });
                        } catch (RejectedExecutionException e) {
                            reconnectTaskStartedMs = 0L;
                        }
                    }
                }
            } catch (Exception e) {
                log.error("runBaseLoop: unexpected exception — loop continues", e);
            }

            // Sleep unconditionally at the end of every cycle to cap the loop at ~100 iterations/sec.
            if (!isShuttingDown && !Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        log.debug("Mesh loop for destination {} exiting.", baseDestination.getName());
    }

    /** Kick the DATA announce/reconnect cycle within ~5s (mirrors triggerImmediateAnnounce()). */
    public void triggerImmediateDataAnnounce() {
        this.lastDataLoopAnnounceMs = System.currentTimeMillis() - DATA_LOOP_ANNOUNCE_INTERVAL_MS + 5_000L;
    }

    // "main" loop for dataDestination (QDN tasks) — mirrors runBaseLoop() for DATA aspect
    private void runDataLoop() {
        while (!isShuttingDown && !Thread.currentThread().isInterrupted()) {
            try {
                final List<ReticulumPeer> peersThisRound = Stream.concat(
                        this.getActiveImmutableLinkedPeers().stream()
                                .filter(p -> p.getPeerAspect() == RNSCommon.PeerAspect.DATA),
                        this.getImmutableIncomingPeers().stream()
                                .filter(p -> p.getPeerAspect() == RNSCommon.PeerAspect.DATA)
                                .filter(p -> {
                                    var pl = p.getPeerLink();
                                    return nonNull(pl) && pl.getStatus() == ACTIVE;
                                })
                ).collect(Collectors.toList());

                final Long now = NTP.getTime();
                for (ReticulumPeer peer : peersThisRound) {
                    ExecuteProduceConsume.Task task;
                    // DATA messages are routed to NetworkData.onMessage() via MessageTask(NETWORKDATA)
                    while ((task = peer.getMessageTask(Peer.NETWORKDATA)) != null) {
                        final ExecuteProduceConsume.Task t = task;
                        try {
                            rnsWorkerPool.execute(() -> {
                                try {
                                    t.perform();
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                } catch (Exception e) {
                                    log.warn("Reticulum DATA worker task threw: {}", e.getMessage(), e);
                                }
                            });
                        } catch (RejectedExecutionException e) {
                            log.warn("[{}] Reticulum DATA worker pool rejected message task", peer.getPeerConnectionId());
                            break;
                        }
                    }

                    ExecuteProduceConsume.Task pingTask = peer.getPingTask(now);
                    if (pingTask != null) {
                        final ExecuteProduceConsume.Task pt = pingTask;
                        try {
                            rnsWorkerPool.execute(() -> {
                                try {
                                    pt.perform();
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                } catch (Exception e) {
                                    log.warn("Reticulum DATA ping task threw: {}", e.getMessage(), e);
                                }
                            });
                        } catch (RejectedExecutionException e) {
                            log.warn("[{}] Reticulum DATA worker pool rejected ping task", peer.getPeerConnectionId());
                        }
                    }
                }

                long nowMs = System.currentTimeMillis();

                // Periodic DATA announce
                if (nowMs - lastDataLoopAnnounceMs >= DATA_LOOP_ANNOUNCE_INTERVAL_MS) {
                    lastDataLoopAnnounceMs = nowMs;
                    if (dataAnnounceTaskStartedMs == 0L) {
                        dataAnnounceTaskStartedMs = nowMs;
                        try {
                            dataAnnounceTaskFuture = dataAnnounceExecutor.submit(() -> {
                                Thread.interrupted();
                                try {
                                    maybeAnnounce(dataDestination, RNSCommon.PeerAspect.DATA);
                                } catch (Exception e) {
                                    log.warn("Exception in data loop announce: {}", e.getMessage(), e);
                                } finally {
                                    if (dataAnnounceTaskStartedMs != 0L) {
                                        dataAnnounceTaskStartedMs = 0L;
                                    }
                                }
                            });
                        } catch (RejectedExecutionException e) {
                            dataAnnounceTaskStartedMs = 0L;
                        }
                    }
                }

                // Periodic DATA peer reconnect
                if (nowMs - lastDataLoopReconnectMs >= DATA_LOOP_RECONNECT_INTERVAL_MS) {
                    lastDataLoopReconnectMs = nowMs;
                    if (dataReconnectTaskStartedMs == 0L) {
                        dataReconnectTaskStartedMs = nowMs;
                        final int activeData = (int) getActiveImmutableLinkedPeers().stream()
                                .filter(p -> p.getPeerAspect() == RNSCommon.PeerAspect.DATA).count();
                        final List<ReticulumPeer> currentDataLinked = getImmutableLinkedPeers().stream()
                                .filter(p -> p.getPeerAspect() == RNSCommon.PeerAspect.DATA)
                                .collect(Collectors.toList());
                        final Set<String> dataTargets = dataPeerStore.reconnectTargets();
                        try {
                            dataReconnectTaskFuture = dataReconnectExecutor.submit(() -> {
                                Thread.interrupted();
                                try {
                                    if (activeData < MIN_DESIRED_DATA_PEERS && !dataTargets.isEmpty()) {
                                        log.info("Active DATA peers {} < desired {} (data loop); requesting paths to {} known peers",
                                                activeData, MIN_DESIRED_DATA_PEERS, dataTargets.size());
                                        for (String hashHex : dataTargets) {
                                            try {
                                                byte[] dhash = decodeHex(hashHex);
                                                boolean tracked = currentDataLinked.stream()
                                                        .anyMatch(p -> Arrays.equals(p.getDestinationHash(), dhash));
                                                if (tracked) continue;
                                                long lastFailure = pendingDataLinkFailureMs.getOrDefault(hashHex, 0L);
                                                boolean recentlyFailed = (System.currentTimeMillis() - lastFailure) < pendingBackoffMs(hashHex);
                                                Identity cachedIdentity = recentlyFailed ? null
                                                        : IdentityKnownDestination.recall(dhash);
                                                if (cachedIdentity != null) {
                                                    log.info("DATA: proactively connecting to {} via cached identity", hashHex);
                                                    createLinkedDataPeerFromIdentity(dhash, cachedIdentity);
                                                } else {
                                                    if (recentlyFailed)
                                                        log.info("DATA: backing off to requestPath for {} (recent PENDING failure)", hashHex);
                                                    else
                                                        log.info("DATA: requestPath for {} (no cached identity)", hashHex);
                                                    Transport.getInstance().requestPath(dhash);
                                                }
                                            } catch (Exception e) {
                                                log.warn("DATA path request/reconnect failed for {}: {}", hashHex, e.getMessage());
                                            }
                                        }
                                    }
                                } catch (Exception e) {
                                    log.warn("Exception in data loop reconnect: {}", e.getMessage(), e);
                                } finally {
                                    if (dataReconnectTaskStartedMs != 0L) {
                                        dataReconnectTaskStartedMs = 0L;
                                    }
                                }
                            });
                        } catch (RejectedExecutionException e) {
                            dataReconnectTaskStartedMs = 0L;
                        }
                    }
                }

            } catch (Exception e) {
                log.error("runDataLoop: unexpected exception — loop continues", e);
            }

            if (!isShuttingDown && !Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        log.debug("Data mesh loop for destination {} exiting.", dataDestination.getName());
    }

    public void broadcast(Function<ReticulumPeer, Message> peerMessageBuilder) {
        List<ReticulumPeer> allPeers = Stream.concat(
                getActiveImmutableLinkedPeers().stream(),
                getImmutableIncomingPeers().stream()
                        .filter(p -> nonNull(p.getPeerLink()) && p.getPeerLink().getStatus() == ACTIVE)
        ).collect(Collectors.toList());

        for (ReticulumPeer peer : allPeers) {
            if (this.isShuttingDown) {
                return;
            }

            Message message = peerMessageBuilder.apply(peer);

            if (message == null) {
                continue;
            }

            peer.sendMessage(message);
        }
    }

    public void shutdown() {
        this.isShuttingDown = true;
        // Controller calls this unconditionally, so it must tolerate a mesh that never started
        // (no Reticulum stack, or start() refused): the destinations are null in that case, but
        // the executors were still created by the constructor and must be closed either way.
        boolean meshWasStarted = reticulum != null && baseDestination != null;
        if (meshWasStarted) {
            basePeerStore.save();
            dataPeerStore.save();
            log.info("shutting down Reticulum");
            baseDestination.setProofStrategy(ProofStrategy.PROVE_NONE);
            dataDestination.setProofStrategy(ProofStrategy.PROVE_NONE);
        } else {
            log.info("Reticulum mesh was not started — closing worker threads only");
        }

        if (this.rnsBaseThread != null && this.rnsBaseThread.isAlive()) {
            this.rnsBaseThread.interrupt();
            try {
                this.rnsBaseThread.join(5000);
                if (this.rnsBaseThread.isAlive())
                    log.warn("RNS base thread did not terminate in time");
            } catch (InterruptedException e) {
                log.warn("Interrupted while waiting for RNS base thread");
            }
        }
        if (this.rnsDataThread != null && this.rnsDataThread.isAlive()) {
            this.rnsDataThread.interrupt();
            try {
                this.rnsDataThread.join(5000);
                if (this.rnsDataThread.isAlive())
                    log.warn("RNS data thread did not terminate in time");
            } catch (InterruptedException e) {
                log.warn("Interrupted while waiting for RNS data thread");
            }
        }
        
        // gracefully close links of peers that point to us.
        // Iterate the immutable snapshots, not the live synchronized lists: a concurrent
        // add/remove during shutdown would otherwise throw ConcurrentModificationException here
        // and skip the rest of the shutdown (executors, exitHandler).
        for (ReticulumPeer p: getImmutableIncomingPeers()) {
            var pl = p.getPeerLink();
            // && (not &): the status read must not happen when the link is null, or shutdown
            // dies on an NPE before it can stop the executors and run exitHandler().
            if (nonNull(pl) && (pl.getStatus() == ACTIVE)) {
                p.sendCloseToRemote(pl);
            }
        }
        log.debug("Shutdown of incomingPeers completed");
        // Disconnect peers gracefully and terminate Reticulum
        for (ReticulumPeer p: getImmutableLinkedPeers()) {
            log.info("shutting down peer: {}", encodeHexString(p.getDestinationHash()));
            p.shutdown();
        }
        log.debug("Shutdown of linkedPeers completed");
        // Shut down worker pool so its threads don't prevent JVM exit
        this.rnsWorkerPool.shutdown();
        this.announceExecutor.shutdown();
        this.reconnectExecutor.shutdown();
        this.dataAnnounceExecutor.shutdown();
        this.dataReconnectExecutor.shutdown();
        this.gatewayManager.shutdown();
        try {
            if (!this.rnsWorkerPool.awaitTermination(2, TimeUnit.SECONDS))
                this.rnsWorkerPool.shutdownNow();
            if (!this.announceExecutor.awaitTermination(2, TimeUnit.SECONDS))
                this.announceExecutor.shutdownNow();
            if (!this.reconnectExecutor.awaitTermination(2, TimeUnit.SECONDS))
                this.reconnectExecutor.shutdownNow();
            if (!this.dataAnnounceExecutor.awaitTermination(2, TimeUnit.SECONDS))
                this.dataAnnounceExecutor.shutdownNow();
            if (!this.dataReconnectExecutor.awaitTermination(2, TimeUnit.SECONDS))
                this.dataReconnectExecutor.shutdownNow();
        } catch (InterruptedException e) {
            this.rnsWorkerPool.shutdownNow();
            this.announceExecutor.shutdownNow();
            this.reconnectExecutor.shutdownNow();
            this.dataAnnounceExecutor.shutdownNow();
            this.dataReconnectExecutor.shutdownNow();
        }

        if (!meshWasStarted) {
            return;
        }

        // exitHandler() can block indefinitely if a zombie link's channel holds a lock
        // (library ABBA deadlock). Run it on a daemon thread with a timeout so the JVM
        // can exit even if the library gets stuck.
        Thread exitThread = new Thread(reticulum::exitHandler, "rns-exit");
        exitThread.setDaemon(true);
        exitThread.start();
        try {
            exitThread.join(5000);
            if (exitThread.isAlive()) {
                log.warn("exitHandler did not complete in 5s — zombie channel likely; forcing shutdown");
            }
        } catch (InterruptedException e) {
            log.warn("Interrupted while waiting for exitHandler");
        }
        log.info("shutdown of Reticulum complete");
    }

    public void baseClientConnected(Link link) {
        log.info("baseClientConnected - link hash: {}, {}", link.getHash(), encodeHexString(link.getHash()));
        ReticulumPeer newPeer = new ReticulumPeer(link);
        newPeer.setPeerLinkHash(link.getHash());
        newPeer.setPeerAspect(RNSCommon.PeerAspect.BASE);
        newPeer.setMessageMagic(getMessageMagic());
        // Capture the initiator's identity once it identifies over the link (see ReticulumPeer's
        // initiator-side link.identify()). Until this fires, an inbound peer has no remote identity
        // and identity-based dedup cannot collapse duplicate inbound links from the same remote.
        link.setRemoteIdentifiedCallback((l, id) -> onIncomingPeerIdentified(newPeer, id));
        // createPeerBuffer() rather than getOrInitPeerBuffer() — avoids synchronized(link)
        // contention on the broadcast path (see ReticulumPeer.createPeerBuffer javadoc).
        newPeer.createPeerBuffer();
        addIncomingPeer(newPeer);
        log.info("***> Base client connected, base link: {}", encodeHexString(link.getLinkId()));
    }

    public void dataClientConnected(Link link) {
        log.info("dataClientConnected - link hash: {}, {}", link.getHash(), encodeHexString(link.getHash()));
        ReticulumPeer newPeer = new ReticulumPeer(link);
        newPeer.setPeerLinkHash(link.getHash());
        newPeer.setPeerAspect(RNSCommon.PeerAspect.DATA);
        newPeer.setMessageMagic(getMessageMagic());
        // See baseClientConnected: resolve the initiator's identity for identity-based dedup.
        link.setRemoteIdentifiedCallback((l, id) -> onIncomingPeerIdentified(newPeer, id));
        newPeer.createPeerBuffer();
        addIncomingPeer(newPeer);
        log.info("***> Data Client connected, data link: {}", encodeHexString(link.getLinkId()));
    }

    // ── reticulumAnnounceGateway: send / receive / dispatch ──────────────────

    /**
     * Build the appData attached to every announce: this node's version, plus a gateway record when
     * this node advertises one. Encoding lives in {@link RNSAnnounceCodec}; the policy decision of
     * <i>whether</i> to advertise stays here, with the settings it depends on.
     */
    private byte[] buildAnnounceAppData() {
        return RNSAnnounceCodec.encode(
                Controller.getInstance().getVersionStringWithoutPrefix(),
                gatewayHostToAdvertise(),
                TARGET_PORT);
    }

    /** The gateway host to put in our announces, or null when this node advertises no gateway. */
    private String gatewayHostToAdvertise() {
        if (!Settings.getInstance().getReticulumAnnounceGateway()) return null;
        if (!Settings.getInstance().getReticulumIsGateway())       return null;

        String host = gatewayManager.getAdvertiseHost();
        // The codec drops a host it cannot encode; say so, or the gateway would silently never be
        // advertised. +1 (hostLen) +2 (port) must fit a single-byte TLV length.
        if (host != null && host.getBytes(StandardCharsets.UTF_8).length > 252) {
            log.warn("Skipping gateway appData: host '{}' is too long to encode (max 252 bytes)", host);
            return null;
        }
        return host;
    }

    /** Record a peer's announced version, keyed by identity hash, for later lookup by incoming peers. */
    private void cacheAnnouncedVersion(Identity identity, String version) {
        if (identity == null || identity.getHash() == null || version == null) return;
        announcedVersions.put(encodeHexString(identity.getHash()), version);
    }

    /** Announced version for a remote identity, or null if we haven't seen its announce yet. */
    public String getAnnouncedVersion(Identity identity) {
        if (identity == null || identity.getHash() == null) return null;
        return announcedVersions.get(encodeHexString(identity.getHash()));
    }

    private class QAnnounceHandler implements AnnounceHandler {
        final String aspectFilter;

        QAnnounceHandler(String aspectFilter) {
            this.aspectFilter = aspectFilter;
        }

        @Override
        public String getAspectFilter() {
            // Return null so Transport fires this handler for ALL received announces.
            // Transport's hash-based filter (hashFromNameAndIdentity(aspectFilter, recall(hash)))
            // fails whenever recall() returns null for the incoming announce identity — the
            // computed hash (no identity component) never matches the actual destination hash,
            // so receivedAnnounce() is never called. We filter by name inside the handler instead.
            return null;
        }

        @Override
        // Serialises announce processing per handler instance. Note the two instances (BASE, DATA)
        // hold separate Lombok $locks, so this does NOT serialise the aspects against each other —
        // it only stops one aspect's announces from interleaving with themselves. Cheap now that
        // the gateway dial (a TCP connect) runs on RNSGatewayManager's executor rather than here,
        // on Reticulum's announce-delivery thread.
        @Synchronized
        public void receivedAnnounce(byte[] destinationHash,
                                     Identity announcedIdentity,
                                     byte[] appData,
                                     byte[] announcePacketHash,
                                     boolean isPathResponse) {
            var peerExists = false;
            var activePeerCount = 0;

            log.debug("Received an announce from {}", encodeHexString(destinationHash));

            // Since getAspectFilter() returns null (match-all), we must verify manually.
            // Recompute the expected hash for "qortal.core" + the announced identity and
            // compare; skip announces that belong to other apps/aspects.
            var expectedHash = hashFromNameAndIdentity(this.aspectFilter, announcedIdentity);
            if (!Arrays.equals(destinationHash, expectedHash)) {
                log.debug("Announce hash mismatch — identity={}, dest={}, expected={}",
                        announcedIdentity != null ? encodeHexString(announcedIdentity.getHash()) : "null",
                        encodeHexString(destinationHash),
                        encodeHexString(expectedHash));
                return;
            }

            String announcedVersion = null;
            if (nonNull(appData)) {
                RNSAnnounceCodec.AnnounceInfo info = RNSAnnounceCodec.decode(appData);
                announcedVersion = info.getVersion();
                // If the announce advertises a Qortal gateway, optionally dial it as a dynamic
                // backbone client interface.
                if (info.hasGateway()) {
                    gatewayManager.maybeAddDynamicGateway(info.getGatewayHost(), info.getGatewayPort());
                }
                // Cache the announced version so incoming peers (no announce at construction) can
                // resolve it once they identify.
                if (announcedVersion != null) {
                    cacheAnnouncedVersion(announcedIdentity, announcedVersion);
                }
            }

            // Enforce minPeerVersion: skip peers that announce a version below the configured
            // minimum (unless the operator allows older peers). Unknown/unparseable versions are
            // NOT skipped — only a known, parseable, below-minimum version is rejected.
            if (announcedVersion != null && !Settings.getInstance().getAllowConnectionsWithOlderPeerVersions()) {
                long announced = RNSAnnounceCodec.parseVersionToLong(announcedVersion);
                long minVersion = RNSAnnounceCodec.parseVersionToLong(Settings.getInstance().getMinPeerVersion());
                if (announced != 0 && minVersion != 0 && announced < minVersion) {
                    log.info("Skipping announce from {} — version {} < minPeerVersion {}",
                            encodeHexString(destinationHash), announcedVersion,
                            Settings.getInstance().getMinPeerVersion());
                    return;
                }
            }

            // add to peer list if we can use more peers
            boolean isDataAspect = QDN_ASPECT.equals(this.aspectFilter);
            int peerLimit = isDataAspect ? MIN_DESIRED_DATA_PEERS : MIN_DESIRED_CORE_PEERS;
            RNSCommon.PeerAspect matchAspect = isDataAspect ? RNSCommon.PeerAspect.DATA : RNSCommon.PeerAspect.BASE;
            var lps =  RNS.getInstance().getImmutableLinkedPeers();
            for (ReticulumPeer p: lps) {
                var pl = p.getPeerLink();
                if (nonNull(pl) && pl.getStatus() == ACTIVE && p.getPeerAspect() == matchAspect) {
                    activePeerCount = activePeerCount + 1;
                }
            }
            if (activePeerCount < peerLimit) {
                for (ReticulumPeer p: lps) {
                    if (Arrays.equals(p.getDestinationHash(), destinationHash)) {
                        log.info("QAnnounceHandler - peer exists - found peer matching destinationHash");
                        if (nonNull(p.getPeerLink())) {
                            log.info("peer link: {}, status: {}",
                                    encodeHexString(p.getPeerLink().getLinkId()), p.getPeerLink().getStatus());
                        }
                        peerExists = true;
                        if (nonNull(p.getPeerLink()) && (p.getPeerLink().getStatus() == CLOSED)) {
                            // Only re-initiate for CLOSED links. PENDING links are already
                            // connecting — creating a second link would race with the first
                            // and the first's TIMEOUT callback would set peerTimedOut=true,
                            // poisoning the peer and triggering premature pruning.
                            p.getOrInitPeerLink();
                        }
                        break;
                    } else {
                        if (nonNull(p.getPeerLink())) {
                            log.debug("QAnnounceHandler - other peer - link: {}, status: {}",
                                    encodeHexString(p.getPeerLink().getLinkId()), p.getPeerLink().getStatus());
                            if (p.getPeerLink().getStatus() == CLOSED) {
                                // mark peer for deletion on next pruning
                                p.setDeleteMe(true);
                            }
                        } else {
                            log.info("QAnnounceHandler - peer link is null");
                        }
                    }
                }
                if (!peerExists) {
                    ReticulumPeer newPeer = getNewPeer(destinationHash, announcedIdentity, announcedVersion);
                    addLinkedPeer(newPeer);
                    log.info("added new {} ReticulumPeer, destinationHash: {}, version: {}",
                            newPeer.getPeerAspect(), encodeHexString(destinationHash), announcedVersion);
                }
            }
        }

        private ReticulumPeer getNewPeer(byte[] destinationHash, Identity announcedIdentity, String announcedVersion) {
            boolean isDataAspect = QDN_ASPECT.equals(this.aspectFilter);
            RNSCommon.PeerAspect aspect = isDataAspect ? RNSCommon.PeerAspect.DATA : RNSCommon.PeerAspect.BASE;
            // Aspect is set by the constructor; setIsDataPeer() is only a setPeerAspect() wrapper.
            ReticulumPeer newPeer = new ReticulumPeer(destinationHash, aspect);
            newPeer.setServerIdentity(announcedIdentity);
            newPeer.setIsInitiator(true);
            newPeer.setMessageMagic(getMessageMagic());
            // Version advertised in the announce appData (may be null if not present); surfaced via
            // /peers/reticulum. Display-only — the numeric min-version gate is unaffected.
            if (announcedVersion != null) {
                newPeer.setPeersVersionString(announcedVersion);
            }
            log.debug(">>> ReticulumPeer created - PeerData: {} - {}", newPeer.getPeerData().toString(), newPeer.getPeerAddress().getDestinationHash());
            return newPeer;
        }
    }

    // Create and add a BASE ReticulumPeer directly from a cached identity (no announce needed).
    // Called from runBaseLoop() when recall() finds the identity in the local known-destinations DB.
    //
    // The ReticulumPeer constructor calls initPeerLink() (which sends the LINK OPEN via outbound()).
    // Do NOT call getOrInitPeerLink() here: the peer's link is already PENDING, so getOrInitPeerLink()
    // would call initPeerLink() a second time — creating a zombie PENDING link in the Reticulum library.
    // The zombie establishes on the remote end (adding a spurious incoming peer there), and when it
    // times out it fires expirePath() → tablesLastCulled=EPOCH → cascading 60-120s cull cycles.
    private void createLinkedPeerFromIdentity(byte[] destinationHash, Identity identity) {
        ReticulumPeer newPeer = new ReticulumPeer(destinationHash);
        newPeer.setServerIdentity(identity);
        newPeer.setIsInitiator(true);
        newPeer.setPeerAspect(RNSCommon.PeerAspect.BASE);
        newPeer.setMessageMagic(getMessageMagic());
        addLinkedPeer(newPeer);
        log.info("Proactively connecting to known peer {} via cached identity", encodeHexString(destinationHash));
        // Link already created in constructor — do NOT call getOrInitPeerLink() here.
        // Detect immediate send failure: ReticulumPeer() → initPeerLink() → new Link() → packet.send()
        // → outbound() is synchronous; if the LINKREQUEST couldn't be sent (no route, backbone down),
        // the link is already CLOSED by the time we get here. Record a failure so the reconnect loop
        // backs off to requestPath() rather than creating a new CLOSED link on every 15s cycle.
        Link lnk = newPeer.getPeerLink();
        if (lnk != null && lnk.getStatus() == CLOSED) {
            log.warn("createLinkedPeerFromIdentity: LINKREQUEST to {} failed immediately — switching to requestPath backoff",
                    encodeHexString(destinationHash));
            recordPendingFailure(encodeHexString(destinationHash), pendingLinkFailureMs);
        }
    }

    // Mirror of createLinkedPeerFromIdentity() for DATA-aspect peers.
    private void createLinkedDataPeerFromIdentity(byte[] destinationHash, Identity identity) {
        ReticulumPeer newPeer = new ReticulumPeer(destinationHash, RNSCommon.PeerAspect.DATA);
        newPeer.setServerIdentity(identity);
        newPeer.setIsInitiator(true);
        newPeer.setMessageMagic(getMessageMagic());
        addLinkedPeer(newPeer);
        log.info("DATA: proactively connecting to known peer {} via cached identity", encodeHexString(destinationHash));
        Link lnk = newPeer.getPeerLink();
        if (lnk != null && lnk.getStatus() == CLOSED) {
            log.warn("createLinkedDataPeerFromIdentity: LINKREQUEST to {} failed immediately — switching to requestPath backoff",
                    encodeHexString(destinationHash));
            recordPendingFailure(encodeHexString(destinationHash), pendingDataLinkFailureMs);
        }
    }

    private static class SingletonContainer {
        private static final RNS INSTANCE = new RNS();
    }

    public static RNS getInstance() {
        return SingletonContainer.INSTANCE;
    }

    public List<ReticulumPeer> getActiveImmutableLinkedPeers() {
        // Plain ArrayList: this is a private snapshot returned to a single caller, never shared.
        // The old Collections.synchronizedList wrapper implied a thread-safety contract that the
        // snapshot does not need, on a value allocated ~200 times/second by the two loops.
        List<ReticulumPeer> activePeers = new ArrayList<>();
        for (ReticulumPeer p: this.immutableLinkedPeers) {
            // Exclude peers marked for removal (deleteMe=true): their buffer is dead even if
            // the library-level link is still ACTIVE. Excluding them lets runBaseLoop() see
            // the real active count and trigger reconnect without waiting for prunePeers().
            if (nonNull(p.getPeerLink()) && (p.getPeerLink().getStatus() == ACTIVE) && !p.getDeleteMe()) {
                activePeers.add(p);
            }
        }
        return activePeers;
    }

    /**
     * Immediately remove a peer from the peer list and kick reconnect, rather than waiting
     * for the next prunePeers() cycle (~60s). Called from ReticulumPeer.peerBufferReady()
     * on read error. Runs on the rnsWorkerPool to avoid blocking the Reticulum callback thread.
     */
    void markPeerForImmediateRemoval(ReticulumPeer peer) {
        if (this.isShuttingDown) return;
        try {
            rnsWorkerPool.submit(() -> {
                peer.makePeerUnavailable();
                if (Boolean.TRUE.equals(peer.getIsInitiator())) {
                    removeLinkedPeer(peer);
                } else {
                    removeIncomingPeer(peer);
                }
                triggerImmediateAnnounce(); // kick runBaseLoop to reconnect within ~5s
            });
        } catch (RejectedExecutionException e) {
            // Pool shut down — prunePeers() will clean up on next cycle
        }
    }

    public void addLinkedPeer(ReticulumPeer peer) {
        // Atomic dedup: receivedAnnounce() and runBaseLoop() can both call this concurrently
        // when a peer drops and reconnects — both see an empty slot and race to fill it.
        synchronized (this.linkedPeers) {
            boolean duplicate = this.linkedPeers.stream()
                    .anyMatch(p -> Arrays.equals(p.getDestinationHash(), peer.getDestinationHash()));
            if (duplicate) {
                log.debug("addLinkedPeer: skipping duplicate for {}", encodeHexString(peer.getDestinationHash()));
                // The loser was built via new ReticulumPeer(dhash), whose constructor already called
                // initPeerLink() and sent the LINKREQUEST — so its Link is live (PENDING/establishing)
                // even though we're discarding the peer. Closing it here prevents a leaked local
                // watchdog thread AND stops the half-formed link from establishing on the remote as a
                // spurious incoming peer (which would itself become a duplicate the remote must prune).
                // The loser always holds its own fresh Link (distinct from the retained peer's), so
                // closing it cannot disturb the live connection. Non-blocking volatile write only.
                peer.closePeerLinkNonBlocking();
                return;
            }
            this.linkedPeers.add(peer);
            this.immutableLinkedPeers = List.copyOf(this.linkedPeers);
        }
        // Hash is added to knownPeerHashes only once the peer's buffer is confirmed ACTIVE
        // (see confirmPeerHash(), called from ReticulumPeer.createPeerBuffer()). This prevents
        // transient/failed connections from accumulating in the persisted peer list.
    }

    public void removePeer(ReticulumPeer peer) {
        if (peer.isInitiator) {
            removeLinkedPeer(peer);
        } else {
            removeIncomingPeer(peer);
        }
    }

    public void removeLinkedPeer(ReticulumPeer peer) {
        peer.shutdownChannel(); // clears channel + nulls peerBuffer; no close() to avoid deadlock
        // NOTE: deliberately does NOT close peerLink. Callers that remove an ACTIVE link must close
        // it themselves, passing the exact Link they decided on (see prunePeers) — closing here
        // would both re-read a peerLink that initPeerLink() may have re-pointed at a fresh Link,
        // and close PENDING links, which triggers the expirePath() cull cascade documented below.
        //
        // Mutation and snapshot rebuild must happen under the same lock addLinkedPeer() holds.
        // Otherwise: this thread reads the backing list, an adder (holding the lock) adds a peer
        // and publishes snapshot {P2}, then this thread publishes its stale snapshot {} — the new
        // peer is in linkedPeers but invisible in immutableLinkedPeers until the next mutation,
        // and every consumer (broadcast, runBaseLoop, prunePeers, findPeerBy*) reads the snapshot.
        synchronized (this.linkedPeers) {
            this.linkedPeers.remove(peer);
            this.immutableLinkedPeers = List.copyOf(this.linkedPeers);
        }
        // Remove from Network's connected/handshaked lists on EVERY removal path. This was
        // previously commented out (and only some callers, e.g. prunePeers, called
        // makePeerUnavailable() explicitly first) — so removal paths that didn't, like
        // RNS.removePeer(), leaked the dead peer into Network.handshakedPeers/connectedPeers
        // forever. Over a churny run that bloated the ping-scan lists and grew Network-Scheduler
        // CPU without bound (test-20/21 wadin). Idempotent, so a double call with an explicit
        // makePeerUnavailable() before the call is harmless. No RNS lock is held here, so acquiring
        // Network's peer-list locks cannot deadlock.
        peer.makePeerUnavailable();
    }

    public void addIncomingPeer(ReticulumPeer peer) {
        // Dedup by remote identity + aspect: evict any existing incoming peer from the same
        // node with the same aspect. Called from linkEstablished() where identity is known.
        // Using CORE_ASPECT for both aspects would incorrectly match CORE/DATA peer pairs
        // from the same remote node and evict the wrong one.
        Identity newId = peer.getServerIdentity();
        String newAspect = (peer.getPeerAspect() == RNSCommon.PeerAspect.DATA) ? QDN_ASPECT : CORE_ASPECT;
        synchronized (this.incomingPeers) {
            if (newId != null) {
                byte[] newHash = hashFromNameAndIdentity(newAspect, newId);
                Iterator<ReticulumPeer> it = this.incomingPeers.iterator();
                while (it.hasNext()) {
                    ReticulumPeer existing = it.next();
                    Identity existingId = existing.getServerIdentity();
                    String existingAspect = (existing.getPeerAspect() == RNSCommon.PeerAspect.DATA) ? QDN_ASPECT : CORE_ASPECT;
                    if (existingId != null && existingAspect.equals(newAspect)
                            && Arrays.equals(hashFromNameAndIdentity(existingAspect, existingId), newHash)) {
                        log.info("addIncomingPeer: replacing stale {} incoming peer from {}",
                                newAspect, encodeHexString(newHash));
                        it.remove();
                        existing.shutdownChannel();
                        // The superseded peer always holds a different Link object than the
                        // replacement (both baseClientConnected and dataClientConnected build a
                        // fresh ReticulumPeer per incoming Link), so closing it cannot disturb the
                        // new peer. Without this its watchdog thread leaks (see removeLinkedPeer).
                        closeIfActive(existing);
                    }
                }
            }
            this.incomingPeers.add(peer);
            this.immutableIncomingPeers = List.copyOf(this.incomingPeers);
        }
    }

    /**
     * Called from the inbound link's remoteIdentified callback (registered in baseClientConnected/
     * dataClientConnected) once the initiator has identified itself via link.identify(). Records the
     * resolved remote identity on the peer — the constructor could not, because the handshake hadn't
     * completed and getRemoteIdentity() was null then — so identity-based dedup finally has a key to
     * work with. Then collapses any older duplicate inbound links from the same remote+aspect,
     * keeping this newly-identified one.
     */
    public void onIncomingPeerIdentified(ReticulumPeer peer, Identity identity) {
        if (identity == null) return;
        peer.setServerIdentity(identity);
        // Now that we know the remote identity, attach its announced version (if we've heard its
        // announce) so /peers/reticulum shows the real version for inbound peers too.
        String version = getAnnouncedVersion(identity);
        if (version != null) {
            peer.setPeersVersionString(version);
        }
        log.info("inbound {} peer identified as {} (link {}), version {}",
                peer.getPeerAspect(), encodeHexString(identity.getHash()),
                peer.getPeerLink() != null ? encodeHexString(peer.getPeerLink().getLinkId()) : "null",
                version);
        dedupIncomingPeerByIdentity(peer);
    }

    /**
     * Proactively evict duplicate incoming peers as soon as the remote identity is known.
     * <p>
     * {@link #addIncomingPeer} runs at link-construction time (from baseClientConnected/
     * dataClientConnected) when {@code getRemoteIdentity()} is still null because the handshake
     * hasn't completed — so its identity-based dedup is skipped and multiple incoming links from
     * the same remote+aspect accumulate until the next {@link #prunePeers} cycle (~60s). This is
     * called from {@link ReticulumPeer#linkEstablished} once {@code serverIdentity} resolves, so
     * redundant links are dropped within seconds instead. The {@code keep} peer (the just-
     * established link) is retained; every other incoming peer with the same identity+aspect is
     * removed. Runs on rnsWorkerPool to avoid mutating the peer list from the Reticulum I/O thread
     * (same discipline as {@link #markPeerForImmediateRemoval}). The prunePeers() pass remains as a
     * backstop.
     */
    public void dedupIncomingPeerByIdentity(ReticulumPeer keep) {
        if (this.isShuttingDown) return;
        Identity keepId = keep.getServerIdentity();
        if (keepId == null) return;
        String keepAspect = (keep.getPeerAspect() == RNSCommon.PeerAspect.DATA) ? QDN_ASPECT : CORE_ASPECT;
        byte[] keepHash = hashFromNameAndIdentity(keepAspect, keepId);
        try {
            rnsWorkerPool.submit(() -> {
                List<ReticulumPeer> toRemove = new ArrayList<>();
                synchronized (this.incomingPeers) {
                    for (ReticulumPeer p : this.incomingPeers) {
                        if (p == keep) continue;
                        Identity pid = p.getServerIdentity();
                        if (pid == null) continue;
                        String pAspect = (p.getPeerAspect() == RNSCommon.PeerAspect.DATA) ? QDN_ASPECT : CORE_ASPECT;
                        if (pAspect.equals(keepAspect)
                                && Arrays.equals(hashFromNameAndIdentity(pAspect, pid), keepHash)) {
                            toRemove.add(p);
                        }
                    }
                }
                // removeIncomingPeer() mutates the list itself, so evict outside the loop/lock above.
                for (ReticulumPeer p : toRemove) {
                    log.info("dedupIncomingPeerByIdentity: removing duplicate {} incoming peer from {}",
                            keepAspect, encodeHexString(keepHash));
                    removeIncomingPeer(p);
                }
            });
        } catch (RejectedExecutionException e) {
            // Pool shut down — prunePeers() will clean up on next cycle
        }
    }

    /**
     * Closes a discarded peer's Link if — and only if — it is still ACTIVE, so its watchdog thread
     * can exit. Plain volatile write, no lock, so it cannot deadlock with the Reticulum receive
     * thread (the ABBA inversion avoided elsewhere in this class).
     * <p>
     * ACTIVE-only is deliberate. A PENDING link closed here would be picked up by Transport's
     * jobs() loop as a CLOSED entry in pendingLinks, which calls expirePath() and resets
     * tablesLastCulled — cascading routing-table culls that hold the Transport lock for minutes.
     * PENDING, HANDSHAKE and STALE watchdogs all reach CLOSED on their own via the establishment
     * or stale timeout, so they never leak; only ACTIVE orphans do.
     */
    private void closeIfActive(ReticulumPeer peer) {
        var link = peer.getPeerLink();
        if (nonNull(link) && link.getStatus() == ACTIVE) {
            link.setStatus(CLOSED);
        }
    }

    public void removeIncomingPeer(ReticulumPeer peer) {
        peer.shutdownChannel(); // clears channel + nulls peerBuffer; no close() to avoid deadlock
        // A stale-but-still-ACTIVE incoming link is kept alive indefinitely by the remote
        // initiator's keepalives, so its watchdog thread never exits — close it. Incoming peers are
        // built fresh per Link (baseClientConnected/dataClientConnected) and never re-initiated, so
        // reading peerLink here cannot pick up a replacement. Only ACTIVE: see removeLinkedPeer.
        closeIfActive(peer);
        // Same lock as addIncomingPeer() — see removeLinkedPeer() for the stale-snapshot race.
        synchronized (this.incomingPeers) {
            this.incomingPeers.remove(peer);
            this.immutableIncomingPeers = List.copyOf(this.incomingPeers);
        }
        // Incoming BASE peers are also registered in Network's connected/handshaked lists via
        // makePeerAvailable(); remove them here so they don't leak (see removeLinkedPeer).
        peer.makePeerUnavailable();
    }

    public Boolean isUnreachable(ReticulumPeer peer) {
        if (peer.getDeleteMe()) {
            return true;
        }
        var link = peer.getPeerLink();
        if (link == null || link.getStatus() == CLOSED) {
            // No link, or the library/Channel already tore it down (a wedged Channel that hits
            // 'retry count exceeded' closes the Link) — definitively unreachable.
            return true;
        }
        // Liveness from the Link's native keepalive via the (now library-fixed) lastInbound,
        // replacing the app-level Channel ping. If a wedged Channel had killed the link we'd have
        // caught it via CLOSED above, so link-level liveness is a sufficient proxy here.
        var lastInbound = link.getLastInbound();
        if (nonNull(lastInbound) && lastInbound.isBefore(Instant.now().minusMillis(LINK_INBOUND_TIMEOUT_MS))) {
            log.debug("RNS - link is unreachable (no inbound for > {}ms)", LINK_INBOUND_TIMEOUT_MS);
            return true;
        }
        return false;
    }

    /**
     * Incoming peers whose link is missing or not ACTIVE.
     * <p>
     * Iterates the immutable snapshot: for-each over the live {@code Collections.synchronizedList}
     * without holding its monitor throws ConcurrentModificationException as soon as
     * addIncomingPeer()/removeIncomingPeer() runs concurrently, and prunePeers() calls this three
     * times per cycle. The returned list is a private snapshot, so it needs no synchronized wrapper.
     */
    public List<ReticulumPeer> getNonActiveIncomingPeers() {
        List<ReticulumPeer> result = new ArrayList<>();
        for (ReticulumPeer p: getImmutableIncomingPeers()) {
            Link pl = p.getPeerLink();
            if (nonNull(pl)) {
                if (pl.getStatus() != ACTIVE) {
                    result.add(p);
                }
            } else {
                result.add(p);
            }
        }
        return result;
    }

    public void prunePeers() throws DataException {
        // prune initiator peers
        Link pLink;
        List<ReticulumPeer> initiatorPeerList = getImmutableLinkedPeers();
        List<ReticulumPeer> incomingPeerList = getImmutableIncomingPeers();
        int numActiveIncomingPeers = incomingPeerList.size() - getNonActiveIncomingPeers().size();
        log.info("number of links (linkedPeers (active) / incomingPeers (active) before pruning: {} ({}), {} ({})",
                initiatorPeerList.size(), getActiveImmutableLinkedPeers().size(),
                incomingPeerList.size(), numActiveIncomingPeers);
        for (ReticulumPeer p : initiatorPeerList) {
            pLink = p.getPeerLink();
            if (nonNull(pLink)) {
                if (p.getPeerTimedOut()) {
                    // options: keep in case peer reconnects or remove => we'll remove it
                    p.makePeerUnavailable();
                    removeLinkedPeer(p);
                    continue;
                }
                if (pLink.getStatus() == ACTIVE) {
                    // Even ACTIVE links can be zombie: buffer dead (deleteMe=true from
                    // peerBufferReady read error) or silent (no data received for >165s).
                    // Without this check, the ACTIVE continue below bypasses deleteMe entirely.
                    if (isUnreachable(p)) {
                        log.info("Removing unreachable ACTIVE peer ({}): {}",
                                p.getDeleteMe() ? "deleteMe" : "data timeout",
                                encodeHexString(p.getDestinationHash()));
                        p.makePeerUnavailable();
                        // Close this exact Link — not p.getPeerLink(), which initPeerLink() may
                        // already have re-pointed at a fresh Link. An orphaned ACTIVE Link never
                        // dies on its own: its watchdog sends keepalives, the remote answers,
                        // lastInbound advances, so the staleTime check never fires and the status
                        // never reaches CLOSED. Test-17 wadin leaked 16,642 watchdog threads this
                        // way over 2 days (~340/h, RSS 34.8G) before crashing. Safe to close here
                        // because the link is ACTIVE, not PENDING — no expirePath() cull cascade.
                        pLink.setStatus(CLOSED);
                        removeLinkedPeer(p);
                    }
                    continue;
                }
                if ((pLink.getStatus() == CLOSED) || (p.getDeleteMe()))  {
                    p.makePeerUnavailable();
                    p.setDeleteMe(false);
                    removeLinkedPeer(p);
                    continue;
                }
                if (pLink.getStatus() == PENDING) {
                    // Give PENDING links 60s to establish before removing them.
                    // Removing too early races with QAnnounceHandler (which creates a
                    // new link and then finds peerTimedOut=true from the old teardown).
                    // Keeping them forever blocks QAnnounceHandler (peerExists=true,
                    // status != CLOSED, so the announce is silently ignored).
                    long pendingSeconds = Duration.between(
                            p.getCreationTimestamp(), Instant.now()).getSeconds();
                    if (pendingSeconds > 60) {
                        log.info("Removing PENDING link stuck for {}s: {}", pendingSeconds, p);
                        p.makePeerUnavailable();
                        p.setIsPeerAvailable(false);
                        // Record failure so the reconnect loop backs off to requestPath() for this
                        // peer for PENDING_FAILURE_BACKOFF_MS, avoiding the cull cascade.
                        String phex = encodeHexString(p.getDestinationHash());
                        if (p.getPeerAspect() == RNSCommon.PeerAspect.DATA) {
                            recordPendingFailure(phex, pendingDataLinkFailureMs);
                        } else {
                            recordPendingFailure(phex, pendingLinkFailureMs);
                        }
                        removeLinkedPeer(p);
                        // Do NOT call pLink.teardown() here.
                        // teardown() sets status=CLOSED → jobs() finds CLOSED link in pendingLinks
                        // → calls expirePath() → tablesLastCulled=EPOCH → next jobs() does a full
                        // routing table cull (60-120s when announce-flooded). Multiple teardowns
                        // chain into cascading culls that hold the Transport lock for 22+ minutes,
                        // blocking all outbound() / requestPath() calls during that window.
                        // We remove the peer from our own tracking only; the library's zombie PENDING
                        // links have a 774000s (8.9 day) timeout (hopsTo=PATHFINDER_M → no path),
                        // which is harmless compared to the cull cascade.
                    }
                    continue;
                }
            }
        }
        // prune non-initiator peers
        List<ReticulumPeer> inaps = getNonActiveIncomingPeers();
        for (ReticulumPeer p: inaps) {
            // Don't call pLink.teardown() — synchronized(link) can block the Controller
            // scheduler if the Reticulum library is processing on this link. The library
            // handles non-active link cleanup via its own keepalive/watchdog mechanism.
            removeIncomingPeer(p);
        }
        // Dedup ACTIVE incoming peers by remote identity. linkEstablished() resolves the identity
        // (null at construction time because the handshake wasn't complete yet), so by prune time
        // (~60s later) it is available. Keep the newest peer per identity; remove the rest.
        {
            Map<String, List<ReticulumPeer>> byIdentity = new HashMap<>();
            for (ReticulumPeer p : getImmutableIncomingPeers()) {
                Link pl = p.getPeerLink();
                if (nonNull(pl) && pl.getStatus() == ACTIVE) {
                    Identity remoteId = p.getServerIdentity();
                    if (remoteId != null) {
                        String aspect = (p.getPeerAspect() == RNSCommon.PeerAspect.DATA) ? QDN_ASPECT : CORE_ASPECT;
                        String key = encodeHexString(hashFromNameAndIdentity(aspect, remoteId));
                        byIdentity.computeIfAbsent(key, k -> new ArrayList<>()).add(p);
                    }
                }
            }
            for (Map.Entry<String, List<ReticulumPeer>> entry : byIdentity.entrySet()) {
                List<ReticulumPeer> dupes = entry.getValue();
                if (dupes.size() > 1) {
                    // Keep the one with the most recent data; remove the rest
                    dupes.sort((a, b) -> b.getLastAccessTimestamp().compareTo(a.getLastAccessTimestamp()));
                    for (int i = 1; i < dupes.size(); i++) {
                        log.info("prunePeers: removing duplicate ACTIVE incoming peer from {}", entry.getKey());
                        removeIncomingPeer(dupes.get(i));
                    }
                }
            }
        }
        // Prune ACTIVE incoming peers that have gone silent: the initiator moved to a new
        // link so pings stopped flowing, but the old library-level link is still ACTIVE.
        // 165s = 3 missed pings.
        for (ReticulumPeer p : getImmutableIncomingPeers()) {
            Link pl = p.getPeerLink();
            if (nonNull(pl) && pl.getStatus() == ACTIVE && isUnreachable(p)) {
                log.info("Removing stale ACTIVE incoming peer (data timeout): {}", encodeHexString(p.getDestinationHash()));
                removeIncomingPeer(p);
            }
        }
        initiatorPeerList = getImmutableLinkedPeers();
        incomingPeerList = getImmutableIncomingPeers();
        numActiveIncomingPeers = incomingPeerList.size() - getNonActiveIncomingPeers().size();
        log.info("number of links (linkedPeers (active) / incomingPeers (active) after pruning: {} ({}), {} ({})",
                initiatorPeerList.size(), getActiveImmutableLinkedPeers().size(),
                incomingPeerList.size(), numActiveIncomingPeers);
        // announce() and requestPath() are intentionally NOT called here — both involve
        // Reticulum library calls that can block if the library holds a lock. The Controller
        // thread must not block (node hangs, stop.sh hangs). runBaseLoop() handles both on
        // its own thread every 30 seconds.
    }

    public void maybeAnnounce(Destination d, RNSCommon.PeerAspect pa) {
        var activePeers = getActiveImmutableLinkedPeers();
        int corePeerCount = 0;
        int dataPeerCount = 0;
        for (Peer p: activePeers) {
            if (p.isDataPeer()) {
                dataPeerCount++;
            } else {
                corePeerCount++;
            }
        }
        if ((corePeerCount <= MIN_DESIRED_CORE_PEERS) && (pa == RNSCommon.PeerAspect.BASE)) {
            log.info("Active core peers ({}) <= desired core peers ({}). Announcing (dest={})",
                    corePeerCount, MIN_DESIRED_CORE_PEERS, d != null ? encodeHexString(d.getHash()) : "null");
            if (nonNull(d)) {
                long announceT0 = System.currentTimeMillis();
                d.announce(buildAnnounceAppData());
                long announceMs = System.currentTimeMillis() - announceT0;
                // d.announce() always returns null when send=true — see Destination.java:675.
                // Real failures are logged by Packet.java as "No interfaces could process".
                log.info("Announce attempt completed in {}ms", announceMs);
                if (announceMs > 5_000) {
                    log.warn("Announce took {}ms — possible jobsLock contention", announceMs);
                }
            } else {
                log.error("Cannot announce - destination is null");
            }
        }
        if ((dataPeerCount <= MIN_DESIRED_DATA_PEERS) && (pa == RNSCommon.PeerAspect.DATA)) {
            log.info("Active DATA peers ({}) <= desired data peers ({}). Announcing (dest={})",
                    dataPeerCount, MIN_DESIRED_DATA_PEERS, d != null ? encodeHexString(d.getHash()) : "null");
            if (nonNull(d)) {
                long announceT0 = System.currentTimeMillis();
                d.announce(buildAnnounceAppData());
                long announceMs = System.currentTimeMillis() - announceT0;
                log.info("DATA announce attempt completed in {}ms", announceMs);
                if (announceMs > 5_000) {
                    log.warn("DATA announce took {}ms — possible jobsLock contention", announceMs);
                }
            } else {
                log.error("Cannot announce DATA - destination is null");
            }
        }
    }

    // Called from ReticulumPeer.createPeerBuffer() when a peer's buffer is confirmed ACTIVE.
    // Only initiator peers call this (non-initiators have our own destination hash, not the remote's).
    void confirmPeerHash(String hashHex, RNSCommon.PeerAspect aspect) {
        // Peer is ACTIVE — clear any failure/backoff state so a future transient drop starts fresh
        // rather than inheriting a long exponential-backoff window from earlier.
        clearPendingFailure(hashHex);
        KnownPeerStore store = (aspect == RNSCommon.PeerAspect.DATA) ? dataPeerStore : basePeerStore;
        if (store != null) {   // null until start() has run
            store.confirm(hashHex);
        }
    }

    /**
     * Helper methods
     */

    public void onPeersV2Message (Peer peer, Message message) {
        // TODO: Do we do anything for ReticulumPeer (?)
        log.debug("PeersV2Message - received {} message: {}", message.getType(), message);
    }

    public List<PeerData> getAllKnownPeers() {
        return getActiveImmutableLinkedPeers().stream()
                .map(ReticulumPeer::getPeerData)
                .collect(Collectors.toList());
    }

    // Returns all active DATA-aspect ReticulumPeers (both initiator and incoming).
    // Used by NetworkData for outbound QDN dispatch over Reticulum.
    public List<ReticulumPeer> getActiveDataPeers() {
        return Stream.concat(
                getActiveImmutableLinkedPeers().stream()
                        .filter(p -> p.getPeerAspect() == RNSCommon.PeerAspect.DATA),
                getImmutableIncomingPeers().stream()
                        .filter(p -> p.getPeerAspect() == RNSCommon.PeerAspect.DATA)
                        .filter(p -> {
                            var pl = p.getPeerLink();
                            return nonNull(pl) && pl.getStatus() == ACTIVE;
                        })
        ).collect(Collectors.toList());
    }

    public byte[] getMessageMagic() {
        return Settings.getInstance().isTestNet() ? TESTNET_MESSAGE_MAGIC : MAINNET_MESSAGE_MAGIC;
    }

}

