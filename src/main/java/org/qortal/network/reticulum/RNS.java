package org.qortal.network.reticulum;

import io.reticulum.Reticulum;
import io.reticulum.Transport;
import io.reticulum.interfaces.ConnectionInterface;
import io.reticulum.interfaces.backbone.BackboneClientInterface;
import io.reticulum.utils.InterfaceUtils;
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
import static io.reticulum.constant.ReticulumConstant.CONFIG_FILE_NAME;
import lombok.Data;
import lombok.Synchronized;

import org.apache.commons.lang3.StringUtils;
import org.qortal.network.Peer;
import org.qortal.network.message.*;
import org.qortal.repository.DataException;
import org.qortal.settings.Settings;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardCopyOption;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.nonNull;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.commons.codec.binary.Hex.encodeHexString;
import org.qortal.utils.ExecuteProduceConsume;
import org.qortal.utils.NTP;
import org.qortal.utils.NamedThreadFactory;
import org.qortal.data.network.PeerData;
import org.qortal.controller.Controller;

// logging
import lombok.extern.slf4j.Slf4j;

// templates
import com.hubspot.jinjava.Jinjava;
import com.google.common.collect.Maps;

@Data
@Slf4j
public class RNS {

    Reticulum reticulum;
    static final String APP_NAME = Settings.getInstance().isTestNet() ? RNSCommon.TESTNET_APP_NAME: RNSCommon.MAINNET_APP_NAME;
    static final Integer TARGET_PORT = Settings.getInstance().isTestNet() ? RNSCommon.TESTNET_IF_TCP_PORT: RNSCommon.MAINNET_IF_TCP_PORT;
    static final String defaultConfigPath = Settings.getInstance().isTestNet() ? RNSCommon.defaultRNSConfigPathTestnet: RNSCommon.defaultRNSConfigPath;
    static final String CORE_ASPECT = "qortal.core";
    static final String QDN_ASPECT  = "qortal.qdn";
    private final int MIN_DESIRED_CORE_PEERS = Settings.getInstance().getReticulumMinDesiredCorePeers();
    private final int MIN_DESIRED_DATA_PEERS = Settings.getInstance().getReticulumMinDesiredDataPeers();

    public Identity serverIdentity;
    public Destination baseDestination;
    public Destination dataDestination;
    private volatile boolean isShuttingDown = false;
    private volatile boolean meshStarted = false;

    // Confirmed-active peer destination hashes — only added when a peer's buffer is successfully
    // created (ACTIVE confirmed). Persisted to disk on shutdown so the next restart reconnects
    // immediately without waiting for announces. Never includes transient/failed-only peers.
    private final Set<String> knownPeerHashes = Collections.synchronizedSet(new HashSet<>());
    // Hashes loaded from disk on startup (may include stale entries from previous sessions).
    // Used alongside knownPeerHashes for path recovery. Not saved back directly; knownPeerHashes
    // (confirmed this session) is saved instead, which naturally drops stale entries over time.
    private final Set<String> loadedPeerHashes = Collections.synchronizedSet(new HashSet<>());
    private static final String KNOWN_PEERS_FILE = "known_peer_hashes.txt";

    // Tracks hashes of peers whose PENDING links were pruned as stuck (>60 s without establishing).
    // When a peer is unreachable, createLinkedPeerFromIdentity() creates a PENDING link that the
    // Reticulum library times out at ~75 s → expirePath() → 60-120 s cull → cascade.
    // After a stuck-PENDING failure or immediate send failure, we back off to requestPath() for
    // PENDING_FAILURE_BACKOFF_MS so the backbone can provide a fresh announce path.
    private final java.util.concurrent.ConcurrentHashMap<String, Long> pendingLinkFailureMs =
            new java.util.concurrent.ConcurrentHashMap<>();
    private static final long PENDING_FAILURE_BACKOFF_MS = 60_000L; // base backoff (first failure); 60s
    // Consecutive PENDING/link failures per peer hash (BASE and DATA hashes are distinct, so one map
    // serves both). Drives CAPPED EXPONENTIAL backoff: a permanently-unreachable peer (e.g. a mis-
    // configured/partitioned mesh) would otherwise be retried every ~120s forever, each retry firing
    // a PENDING link → expirePath() cull cascade → sustained reconnect-thread CPU. Backoff doubles per
    // failure up to MAX_PENDING_FAILURE_BACKOFF_MS, so stale peers become effectively dormant. Reset on
    // a successful ACTIVE connection (confirmPeerHash) so transient outages aren't penalised long-term.
    private final java.util.concurrent.ConcurrentHashMap<String, Integer> pendingFailureCount =
            new java.util.concurrent.ConcurrentHashMap<>();
    private static final long MAX_PENDING_FAILURE_BACKOFF_MS = 30 * 60_000L; // 30 min cap

    /**
     * Maintain two lists for each subset of peers
     *  => a synchronizedList, modified when peers are added/removed
     *  => an immutable List, automatically rebuild to mirror synchronizedList, served to consumers
     *  linkedPeers are "initiators" (containing initiator reticulum Link), actively doing work.
     *  incomimgPeers are "non-initiators", the passive end of bidirectional Reticulum Buffers.
     */
    private final List<ReticulumPeer> linkedPeers = Collections.synchronizedList(new ArrayList<>());
    private List<ReticulumPeer> immutableLinkedPeers = Collections.emptyList();
    private final List<ReticulumPeer> incomingPeers = Collections.synchronizedList(new ArrayList<>());
    private List<ReticulumPeer> immutableIncomingPeers = Collections.emptyList();

    // ── Gateway announce (reticulumAnnounceGateway) ──────────────────────────
    // When enabled, the node embeds its own backbone server endpoint as an
    // appData payload on each announce; peers receiving the announce can then
    // dynamically add a BackboneClientInterface to discover gateways without
    // every node needing them hardcoded in settings.
    private static final byte[] GW_APPDATA_MAGIC = { 'Q', 'G', 'W', '1' };
    private static final int GW_APPDATA_MIN_LEN = GW_APPDATA_MAGIC.length + 1 /*hostLen*/ + 2 /*port*/;
    private static final Duration GATEWAY_COOLDOWN = Duration.ofMinutes(10);
    /** host:port → last time we considered adding (success or skip), to throttle churn. */
    private final Map<String, Instant> recentGatewayAttempts = new ConcurrentHashMap<>();

    // ── Announce appData container (QAN1) ────────────────────────────────────
    // Self-describing TLV payload attached to every announce. Always carries the node's version;
    // optionally carries a gateway record (superseding the old QGW1-only payload). Extensible:
    // future capability records simply add new type bytes. Decode falls back to legacy QGW1.
    private static final byte[] QAN_APPDATA_MAGIC = { 'Q', 'A', 'N', '1' };
    private static final byte QAN_TLV_VERSION = 0x01; // value = UTF-8 version string (no "qortal-" prefix)
    private static final byte QAN_TLV_GATEWAY = 0x02; // value = [hostLen:1][host][port:2] (legacy QGW1 body)
    /** Announced version keyed by identity hash (hex). Lets incoming peers (which have no announce
     *  at construction) resolve their version once they identify. Bounded LRU to avoid unbounded
     *  growth from mesh-wide announces. */
    private final Map<String, String> announcedVersions = Collections.synchronizedMap(
            new LinkedHashMap<String, String>(64, 0.75f, true) {
                @Override protected boolean removeEldestEntry(Map.Entry<String, String> e) { return size() > 512; }
            });
    /** Local FQDN cached at first use (whatever JDK returns — used only for self-skip). */
    private volatile String localFqdn;
    /** Host this node will advertise (either explicit setting or validated auto-detect). null = don't advertise. */
    private volatile String advertiseHost;
    /** Guards one-time logging of the chosen advertise host. */
    private volatile boolean advertiseHostResolved = false;

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
    private volatile java.util.concurrent.Future<?> announceTaskFuture = null;
    private volatile java.util.concurrent.Future<?> reconnectTaskFuture = null;
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
    private volatile java.util.concurrent.Future<?> dataAnnounceTaskFuture  = null;
    private volatile java.util.concurrent.Future<?> dataReconnectTaskFuture = null;

    // Persisted DATA peer hashes — same semantics as knownPeerHashes / loadedPeerHashes for BASE
    private static final String KNOWN_DATA_PEERS_FILE = "known_data_peer_hashes.txt";
    private final Set<String> knownDataPeerHashes  = Collections.synchronizedSet(new HashSet<>());
    private final Set<String> loadedDataPeerHashes = Collections.synchronizedSet(new HashSet<>());
    private final java.util.concurrent.ConcurrentHashMap<String, Long> pendingDataLinkFailureMs =
            new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * Record a PENDING/link-establishment failure for a peer: stamp the failure time in the
     * aspect-specific time map and increment the shared failure counter (for exponential backoff).
     */
    private void recordPendingFailure(String hashHex,
            java.util.concurrent.ConcurrentHashMap<String, Long> timeMap) {
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
        for (io.reticulum.interfaces.ConnectionInterface iface : Transport.getInstance().getInterfaces()) {
            if (iface instanceof io.reticulum.interfaces.backbone.BackboneClientInterface) {
                ((io.reticulum.interfaces.backbone.BackboneClientInterface) iface).forceReconnect();
            }
        }
    }

    // Constructor
    public RNS () {
        log.info("RNS constructor");
        try {
            log.info("creating config in {}", defaultConfigPath);
            initConfig(defaultConfigPath);
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
        loadKnownPeerHashes();
        loadKnownDataPeerHashes();
        // do a first announce (across all configured interfaces)
        byte[] initialAppData = buildAnnounceAppData();
        baseDestination.announce(initialAppData);
        log.info("Sent initial announce from {} ({})", encodeHexString(baseDestination.getHash()), baseDestination.getName());
        dataDestination.announce(initialAppData);
        log.info("Sent initial announce from {} ({})", encodeHexString(dataDestination.getHash()), dataDestination.getName());
        // Seed loop announce timers. On restart (non-empty loaded hashes) fire path requests
        // at t=15s; on first-ever start use the full 30s window.
        this.lastBaseLoopAnnounceMs = loadedPeerHashes.isEmpty()
                ? System.currentTimeMillis()
                : System.currentTimeMillis() - BASE_LOOP_ANNOUNCE_INTERVAL_MS + 15_000L;
        this.lastDataLoopAnnounceMs = loadedDataPeerHashes.isEmpty()
                ? System.currentTimeMillis()
                : System.currentTimeMillis() - DATA_LOOP_ANNOUNCE_INTERVAL_MS + 15_000L;

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

    private void initConfig(String configDir) throws IOException {
        File configDir1 = new File(configDir);
        if (!configDir1.exists()) {
            configDir1.mkdir();
        }
        var configPath = Path.of(configDir1.getAbsolutePath());
        Path configFile = configPath.resolve(CONFIG_FILE_NAME);
        var localhost = InetAddress.getLocalHost();
        var fqdn = localhost.getCanonicalHostName();
        var isReticulumGateway = Settings.getInstance().getReticulumIsGateway();
        var reticulumDesiredClientInterfaces =  Settings.getInstance().getReticulumDesiredClientInterfaces();
        var reticulumTcpGatewayServers = Arrays.stream(Settings.getInstance().getReticulumTcpGatewayServers()).collect(Collectors.toList());
        var reticulumBackboneGatewayServers = Arrays.stream(Settings.getInstance().getReticulumBackboneGatewayServers()).collect(Collectors.toList());
        reticulumTcpGatewayServers.remove(fqdn);
        reticulumBackboneGatewayServers.remove(fqdn);
        Map<String, Object> context = Maps.newHashMap();

        if (Files.notExists(configFile) || Settings.getInstance().isReticulumRegenerateConfigOnRestart()) {
            try {
                // jinjava variables set in context:
                // * tcp_gateway_servers: list of nodes with a TCPServerInterface
                // * tcp_backbone_servers: list of nodes with a BackboneServerInterface
                // * num_client_interfaces: number of client interfaces to gateways be configured
                // * host_fqdn: host FQDN
                // * qortal_network_name: either "qortal" or "qortaltest" (from isTestnet)
                // * is_reticulum_gateway: one of the instances (Qortal core or RNS) has
                //                         at least one Gateway interface
                // * is_test_net: String "true" or "false" (from isTestNet)
                // * target_port: target port for TCPServerInterface or BackboneServerInterface (only)
                // * use_python_rns: use local shared python rnsd (has to provide a gateway interface)
                // * python_rns_if_port: rnsd TCPServerInterface port (if rnsd gateway is a TCPServerInterface)
                var jnj = new Jinjava();
                var reticulumTcpGateways = StringUtils.join(reticulumTcpGatewayServers, " ");
                var reticulumBackboneGateways = StringUtils.join(reticulumBackboneGatewayServers, " ");
                log.info("reticulumTcpGateways: {}, reticulumBackboneGateways", reticulumTcpGateways);
                context.put("tcp_gateway_servers",  reticulumTcpGateways);
                context.put("backbone_gateway_servers",  reticulumBackboneGateways);
                context.put("num_client_interfaces", reticulumDesiredClientInterfaces);
                context.put("host_fqdn", fqdn);
                String networkName = Settings.getInstance().getReticulumNetworkName();
                context.put("qortal_network_name", networkName.isEmpty() ? APP_NAME : networkName);
                context.put("target_port", TARGET_PORT);
                context.put("is_reticulum_gateway", isReticulumGateway ? "true" : "false");
                context.put("use_python_rns", Settings.getInstance().getReticulumUsePythonRNS() ? "true" : "false");
                context.put("python_rns_if_port", Settings.getInstance().getReticulumPythonRNSGatewayPort());
                context.put("passphrase", Settings.getInstance().getReticulumPassphrase());

                // render config.yml from template
                log.info("Rendering new Reticulum configuration file from resource {}", RNSCommon.jinjaConfigTemplateName  );
                var templateResourceInpuSteam = this.getClass().getClassLoader().getResourceAsStream(RNSCommon.jinjaConfigTemplateName);
                var template = new BufferedReader(new InputStreamReader(templateResourceInpuSteam)).lines().parallel().collect(Collectors.joining("\n"));
                var renderedConfig = jnj.render(template, context);
                // Delete any existing config first. Files.write(CREATE, WRITE) does NOT truncate, so
                // regenerating a SHORTER config (e.g. after lowering reticulumDesiredClientInterfaces)
                // left the old file's trailing bytes in place — a stale/duplicated interface, and
                // sometimes a corrupt tail. Deleting guarantees the rendered file is exactly the new
                // content. (The fallback path below already uses Files.copy REPLACE_EXISTING.)
                Files.deleteIfExists(configFile);
                Files.write(configFile, renderedConfig.getBytes(), CREATE, WRITE);
            } catch (Exception e) {
                log.error("Failed to render config file - creating fallback default  config file", e);
                var defaultConfig = this.getClass().getClassLoader().getResourceAsStream(RNSCommon.defaultRNSConfig);
                if (Settings.getInstance().isTestNet()) {
                    defaultConfig = this.getClass().getClassLoader().getResourceAsStream(RNSCommon.defaultRNSConfigTestnet);
                }
                Files.copy(defaultConfig, configFile, StandardCopyOption.REPLACE_EXISTING);
            }
        } else {
            log.debug("Reticulum config exists, skipping.");
        }
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
                        } catch (java.util.concurrent.RejectedExecutionException e) {
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
                        } catch (java.util.concurrent.RejectedExecutionException e) {
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
                        java.util.concurrent.Future<?> f = announceTaskFuture;
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
                        } catch (java.util.concurrent.RejectedExecutionException e) {
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
                        java.util.concurrent.Future<?> rf = reconnectTaskFuture;
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
                        final Set<String> reconnectTargets = new HashSet<>(knownPeerHashes);
                        reconnectTargets.addAll(loadedPeerHashes);
                        try {
                            reconnectTaskFuture = reconnectExecutor.submit(() -> {
                                Thread.interrupted(); // clear any stale interrupt flag from prior cancel
                                try {
                                    // Log interface online status for diagnostics
                                    for (io.reticulum.interfaces.ConnectionInterface iface : Transport.getInstance().getInterfaces()) {
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
                                                byte[] dhash = org.apache.commons.codec.binary.Hex.decodeHex(hashHex);
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
                        } catch (java.util.concurrent.RejectedExecutionException e) {
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
                        } catch (java.util.concurrent.RejectedExecutionException e) {
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
                        } catch (java.util.concurrent.RejectedExecutionException e) {
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
                        } catch (java.util.concurrent.RejectedExecutionException e) {
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
                        final Set<String> dataTargets = new HashSet<>(knownDataPeerHashes);
                        dataTargets.addAll(loadedDataPeerHashes);
                        try {
                            dataReconnectTaskFuture = dataReconnectExecutor.submit(() -> {
                                Thread.interrupted();
                                try {
                                    if (activeData < MIN_DESIRED_DATA_PEERS && !dataTargets.isEmpty()) {
                                        log.info("Active DATA peers {} < desired {} (data loop); requesting paths to {} known peers",
                                                activeData, MIN_DESIRED_DATA_PEERS, dataTargets.size());
                                        for (String hashHex : dataTargets) {
                                            try {
                                                byte[] dhash = org.apache.commons.codec.binary.Hex.decodeHex(hashHex);
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
                        } catch (java.util.concurrent.RejectedExecutionException e) {
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
        saveKnownPeerHashes();
        saveKnownDataPeerHashes();
        log.info("shutting down Reticulum");
        baseDestination.setProofStrategy(ProofStrategy.PROVE_NONE);
        dataDestination.setProofStrategy(ProofStrategy.PROVE_NONE);

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
        
        // gracefully close links of peers that point to us
        for (ReticulumPeer p: incomingPeers) {
            var pl = p.getPeerLink();
            if (nonNull(pl) & (pl.getStatus() == ACTIVE)) {
                p.sendCloseToRemote(pl);
            }
        }
        log.debug("Shutdown of incomingPeers completed");
        // Disconnect peers gracefully and terminate Reticulum
        for (ReticulumPeer p: linkedPeers) {
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
        try {
            if (!this.rnsWorkerPool.awaitTermination(2, java.util.concurrent.TimeUnit.SECONDS))
                this.rnsWorkerPool.shutdownNow();
            if (!this.announceExecutor.awaitTermination(2, java.util.concurrent.TimeUnit.SECONDS))
                this.announceExecutor.shutdownNow();
            if (!this.reconnectExecutor.awaitTermination(2, java.util.concurrent.TimeUnit.SECONDS))
                this.reconnectExecutor.shutdownNow();
            if (!this.dataAnnounceExecutor.awaitTermination(2, java.util.concurrent.TimeUnit.SECONDS))
                this.dataAnnounceExecutor.shutdownNow();
            if (!this.dataReconnectExecutor.awaitTermination(2, java.util.concurrent.TimeUnit.SECONDS))
                this.dataReconnectExecutor.shutdownNow();
        } catch (InterruptedException e) {
            this.rnsWorkerPool.shutdownNow();
            this.announceExecutor.shutdownNow();
            this.reconnectExecutor.shutdownNow();
            this.dataAnnounceExecutor.shutdownNow();
            this.dataReconnectExecutor.shutdownNow();
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
     * Build the appData attached to every announce: a QAN1 TLV container carrying this node's
     * version (always) and, when this node advertises a gateway, a gateway record. Never returns
     * null — the version always makes it worth sending. Extensible for future capability records.
     */
    private byte[] buildAnnounceAppData() {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        out.write(QAN_APPDATA_MAGIC, 0, QAN_APPDATA_MAGIC.length);

        // VERSION record (always). e.g. "6.1.9-71cfe5b"
        byte[] ver = Controller.getInstance().getVersionStringWithoutPrefix().getBytes(StandardCharsets.UTF_8);
        if (ver.length > 255) ver = Arrays.copyOf(ver, 255);
        out.write(QAN_TLV_VERSION);
        out.write(ver.length);
        out.write(ver, 0, ver.length);

        // GATEWAY record (only when advertising a gateway)
        byte[] gw = buildGatewayValue();
        if (gw != null) {
            out.write(QAN_TLV_GATEWAY);
            out.write(gw.length);
            out.write(gw, 0, gw.length);
        }
        return out.toByteArray();
    }

    /**
     * The gateway record's VALUE bytes: {@code [hostLen:1][host][port:2]}, or {@code null} when
     * this node is not advertising a gateway or the host is unusable. Same body layout as the
     * legacy QGW1 payload (minus its magic), so {@link #decodeGatewayAppData} still parses old peers.
     */
    private byte[] buildGatewayValue() {
        if (!Settings.getInstance().getReticulumAnnounceGateway()) return null;
        if (!Settings.getInstance().getReticulumIsGateway())       return null;

        String host = getAdvertiseHost();
        if (host == null || host.isEmpty()) return null;

        byte[] hostBytes = host.getBytes(StandardCharsets.UTF_8);
        // +1 (hostLen) +2 (port) must fit a single-byte TLV length (<=255)
        if (hostBytes.length < 1 || hostBytes.length > 252) {
            log.warn("Skipping gateway appData: host '{}' encoded length {} not in 1..252",
                    host, hostBytes.length);
            return null;
        }
        int port = TARGET_PORT;
        if (port < 1 || port > 0xFFFF) return null;

        ByteBuffer buf = ByteBuffer.allocate(1 + hostBytes.length + 2);
        buf.put((byte) hostBytes.length);
        buf.put(hostBytes);
        buf.putShort((short) port);
        return buf.array();
    }

    /**
     * Whether a host string is suitable to advertise as a gateway or to dial
     * as a dynamically-announced gateway. Rejects:
     *   - null/empty
     *   - "localhost" (case-insensitive)
     *   - loopback IPv4 (127.x.x.x) and IPv6 (::1)
     *   - bare single-label names with no dot — not an FQDN, not resolvable
     *     for arbitrary peers
     * The check is best-effort: we cannot tell from inside the process whether
     * a name actually resolves for any given peer, only catch the obvious cases
     * that the auto-detection commonly produces on desktops, VMs and NAT'd hosts.
     */
    private static boolean isUsableAdvertiseHost(String host) {
        if (host == null) return false;
        String h = host.trim();
        if (h.isEmpty()) return false;
        if (h.equalsIgnoreCase("localhost")) return false;
        if (h.startsWith("127.")) return false;
        if (h.equals("::1")) return false;
        // Require at least one dot. Catches "dev-vm-2-desktop" and similar
        // local hostnames; both real FQDNs and IPv4/IPv6 literals have dots
        // (or colons — we accept ':' too for raw IPv6, though that is unusual).
        if (h.indexOf('.') < 0 && h.indexOf(':') < 0) return false;
        return true;
    }

    /**
     * Returns the host string this node will advertise (explicit setting, or
     * validated auto-detect), or null if no usable host could be determined.
     * Logs the decision once at first call. Cached for subsequent calls.
     */
    private String getAdvertiseHost() {
        if (advertiseHostResolved) return advertiseHost;

        synchronized (this) {
            if (advertiseHostResolved) return advertiseHost;

            String explicit = Settings.getInstance().getReticulumAnnouncedHost();
            String chosen;
            String source;
            if (explicit != null && !explicit.trim().isEmpty()) {
                chosen = explicit.trim();
                source = "reticulumAnnouncedHost setting";
            } else {
                String detected = getLocalFqdn();
                if (isUsableAdvertiseHost(detected)) {
                    chosen = detected;
                    source = "auto-detected FQDN";
                } else {
                    chosen = null;
                    source = "auto-detected FQDN '" + detected + "' is not usable";
                }
            }

            if (chosen != null) {
                log.info("Reticulum gateway announce: will advertise host '{}:{}' (source: {})",
                        chosen, TARGET_PORT, source);
            } else {
                log.warn("Reticulum gateway announce: no usable host to advertise ({}); "
                        + "set reticulumAnnouncedHost to your publicly-resolvable FQDN to enable",
                        source);
            }
            advertiseHost = chosen;
            advertiseHostResolved = true;
            return chosen;
        }
    }

    /**
     * Decode gateway info from an announce's appData. Returns {@code null} if
     * the payload doesn't start with our magic, is malformed, or carries
     * out-of-range values. Returning null is always safe — caller skips.
     *
     * @return {@code new String[] {host, portStr}} on success, else {@code null}
     */
    private static String[] decodeGatewayAppData(byte[] appData) {
        if (appData == null || appData.length < GW_APPDATA_MIN_LEN) return null;
        for (int i = 0; i < GW_APPDATA_MAGIC.length; i++) {
            if (appData[i] != GW_APPDATA_MAGIC[i]) return null;
        }
        int hostLen = appData[GW_APPDATA_MAGIC.length] & 0xFF;
        int hostStart = GW_APPDATA_MAGIC.length + 1;
        if (hostLen < 1 || appData.length < hostStart + hostLen + 2) return null;
        String host;
        try {
            host = new String(appData, hostStart, hostLen, StandardCharsets.UTF_8);
        } catch (Exception e) {
            return null;
        }
        int port = ((appData[hostStart + hostLen] & 0xFF) << 8)
                 | (appData[hostStart + hostLen + 1] & 0xFF);
        if (port < 1 || port > 0xFFFF) return null;
        return new String[] { host, String.valueOf(port) };
    }

    /** Decoded announce appData: any field may be absent (version null, gwPort 0). */
    private static final class AnnounceInfo {
        String version;
        String gwHost;
        int gwPort;
    }

    /**
     * Decode a QAN1 announce appData container into its fields. Robust to malformed/truncated
     * records (stops parsing). Falls back to the legacy QGW1 gateway-only payload when the QAN1
     * magic is absent, so announces from older peers still yield gateway info (version stays null).
     */
    private static AnnounceInfo decodeAnnounceAppData(byte[] appData) {
        AnnounceInfo info = new AnnounceInfo();
        boolean isQan = appData != null && appData.length >= QAN_APPDATA_MAGIC.length;
        if (isQan) {
            for (int i = 0; i < QAN_APPDATA_MAGIC.length; i++) {
                if (appData[i] != QAN_APPDATA_MAGIC[i]) { isQan = false; break; }
            }
        }
        if (!isQan) {
            String[] gw = decodeGatewayAppData(appData); // legacy QGW1
            if (gw != null) { info.gwHost = gw[0]; info.gwPort = Integer.parseInt(gw[1]); }
            return info;
        }
        int p = QAN_APPDATA_MAGIC.length;
        while (p + 2 <= appData.length) {
            int type = appData[p] & 0xFF;
            int len = appData[p + 1] & 0xFF;
            int vStart = p + 2;
            if (vStart + len > appData.length) break; // truncated record
            if (type == QAN_TLV_VERSION) {
                try { info.version = new String(appData, vStart, len, StandardCharsets.UTF_8); }
                catch (Exception ignored) { }
            } else if (type == QAN_TLV_GATEWAY && len >= 3) {
                int hl = appData[vStart] & 0xFF;
                if (1 + hl + 2 <= len) {
                    try { info.gwHost = new String(appData, vStart + 1, hl, StandardCharsets.UTF_8); }
                    catch (Exception ignored) { }
                    int port = ((appData[vStart + 1 + hl] & 0xFF) << 8) | (appData[vStart + 2 + hl] & 0xFF);
                    if (port >= 1 && port <= 0xFFFF) info.gwPort = port;
                }
            }
            p = vStart + len; // skip unknown types too
        }
        return info;
    }

    /**
     * Parse "x.y.z[-hash]" (with or without the "qortal-" prefix) to the 3-short packed long used
     * for min-version comparison (same scheme as IPPeer). Returns 0 if unparseable.
     */
    static long parseVersionToLong(String versionString) {
        if (versionString == null) return 0L;
        String s = versionString.startsWith(Controller.VERSION_PREFIX)
                ? versionString : Controller.VERSION_PREFIX + versionString;
        Matcher m = ReticulumPeer.VERSION_PATTERN.matcher(s);
        if (!m.lookingAt()) return 0L;
        long v = 0;
        for (int g = 1; g <= 3; g++) {
            long value = Long.parseLong(m.group(g));
            if (value < 0 || value > Short.MAX_VALUE) return 0L;
            v = (v << 16) | value;
        }
        return v;
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

    /**
     * Cached on first use. Reads canonical hostname so a peer hearing our
     * announce echo (looped back via a relay) doesn't trigger a self-connect.
     */
    private String getLocalFqdn() {
        String fqdn = localFqdn;
        if (fqdn == null) {
            try {
                fqdn = InetAddress.getLocalHost().getCanonicalHostName();
            } catch (Exception e) {
                log.warn("Cannot resolve local FQDN for gateway announce: {}", e.getMessage());
                fqdn = "";
            }
            localFqdn = fqdn;
        }
        return fqdn;
    }

    /**
     * Decide whether to dial a peer-advertised gateway and, if yes, dynamically
     * register a {@link BackboneClientInterface}. Enforces:
     *   - self-skip (advertised host == our FQDN)
     *   - dedup against existing interfaces (same target host/port)
     *   - per-endpoint cooldown to prevent churn from repeated announces
     *   - total initiator backbone-client cap = reticulumDesiredClientInterfaces
     *   - IFAC setup using the configured passphrase and network name
     *
     * All failure paths log at WARN/DEBUG and return — never throw to caller.
     */
    private void maybeAddDynamicGateway(String host, int port) {
        if (host == null || host.isEmpty() || port < 1 || port > 0xFFFF) return;
        String endpoint = host + ":" + port;

        // Reject misconfigured peer announces (localhost, loopback, single-label
        // names). Without this guard, a sender whose auto-detected FQDN was bad
        // would have every receiver pointlessly try to dial 127.0.0.1, its own
        // hostname, etc.
        if (!isUsableAdvertiseHost(host)) {
            log.debug("Gateway announce: dropping unusable host '{}' (likely misconfigured peer)", host);
            return;
        }

        // Self-skip: compare against both our advertised name (if any) and our
        // local FQDN (whatever JDK returned, even if not usable for advertising).
        // Belt-and-suspenders so we don't dial ourselves via either route.
        if (host.equalsIgnoreCase(getLocalFqdn())) {
            log.debug("Gateway announce: ignoring self via local FQDN ({}:{})", host, port);
            return;
        }
        String myAdvertise = getAdvertiseHost();
        if (myAdvertise != null && host.equalsIgnoreCase(myAdvertise)) {
            log.debug("Gateway announce: ignoring self via advertised host ({}:{})", host, port);
            return;
        }

        // Cooldown: skip if we've considered this endpoint recently.
        Instant now = Instant.now();
        Instant last = recentGatewayAttempts.get(endpoint);
        if (last != null && Duration.between(last, now).compareTo(GATEWAY_COOLDOWN) < 0) {
            return; // silently — this is the steady-state path on repeated announces
        }
        recentGatewayAttempts.put(endpoint, now);

        // Already have an interface to this endpoint? (Static, prior-dynamic, or
        // matching by autoconnect hash.)
        for (ConnectionInterface iface : Transport.getInstance().getInterfaces()) {
            if (host.equalsIgnoreCase(iface.getTargetHost()) && port == iface.getTargetPort()) {
                log.debug("Gateway {} already has a configured interface; skipping", endpoint);
                return;
            }
        }

        // Cap: count initiator BackboneClientInterface instances (static + dynamic);
        // the user-configured cap caps the total, not just the dynamic share.
        int maxClients = Settings.getInstance().getReticulumDesiredClientInterfaces();
        long currentInitiators = Transport.getInstance().getInterfaces().stream()
                .filter(i -> i instanceof BackboneClientInterface
                        && ((BackboneClientInterface) i).isInitiator())
                .count();
        if (currentInitiators >= maxClients) {
            log.debug("Gateway {}: at client interface cap ({} >= {}); not adding",
                    endpoint, currentInitiators, maxClients);
            return;
        }

        log.info("Dynamically adding announced backbone gateway {} (initiators {}/{})",
                endpoint, currentInitiators + 1, maxClients);

        try {
            BackboneClientInterface iface = new BackboneClientInterface();
            iface.setInterfaceName("Backbone Client Interface qortal " + host + " (announced)");
            iface.setTargetHost(host);
            iface.setTargetPort(port);
            iface.setEnabled(true);

            // IFAC setup — same passphrase as the static interfaces, otherwise
            // the handshake against an IFAC-protected backbone server will fail.
            String networkName = Settings.getInstance().getReticulumNetworkName();
            if (networkName == null || networkName.isEmpty()) networkName = APP_NAME;
            iface.setIfacNetName(networkName);
            String passphrase = Settings.getInstance().getReticulumPassphrase();
            if (passphrase != null && !passphrase.isEmpty()) {
                iface.setIfacNetKey(passphrase);
            }
            if (!InterfaceUtils.initIFac(iface)) {
                log.warn("Gateway {}: IFAC init returned false; aborting dynamic add", endpoint);
                return;
            }

            Transport.getInstance().getInterfaces().add(iface);
            iface.launch();
        } catch (Exception e) {
            log.warn("Gateway {}: failed to register dynamic backbone client interface: {}",
                    endpoint, e.getMessage());
        }
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
                AnnounceInfo info = decodeAnnounceAppData(appData);
                announcedVersion = info.version;
                // If the announce advertises a Qortal gateway, optionally dial it as a dynamic
                // backbone client interface.
                if (info.gwHost != null && info.gwPort > 0) {
                    maybeAddDynamicGateway(info.gwHost, info.gwPort);
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
                long announced = parseVersionToLong(announcedVersion);
                long minVersion = parseVersionToLong(Settings.getInstance().getMinPeerVersion());
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
        List<ReticulumPeer> activePeers = Collections.synchronizedList(new ArrayList<>());
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
        } catch (java.util.concurrent.RejectedExecutionException e) {
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
        this.linkedPeers.remove(peer); // single synchronized operation on the list
        this.immutableLinkedPeers = List.copyOf(this.linkedPeers);
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
        } catch (java.util.concurrent.RejectedExecutionException e) {
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
        this.incomingPeers.remove(peer); // single synchronized operation on the list
        this.immutableIncomingPeers = List.copyOf(this.incomingPeers);
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

    public List<ReticulumPeer> getNonActiveIncomingPeers() {
        var ips = getIncomingPeers();
        List<ReticulumPeer> result = Collections.synchronizedList(new ArrayList<>());
        Link pl;
        for (ReticulumPeer p: ips) {
            pl = p.getPeerLink();
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
                    long pendingSeconds = java.time.Duration.between(
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
            Map<String, List<ReticulumPeer>> byIdentity = new java.util.HashMap<>();
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

    /**
     * Persist known peer destination hashes so a restarted node can call requestPath()
     * immediately rather than waiting up to 15 minutes for a natural announce.
     */
    private void saveKnownPeerHashes() {
        if (reticulum == null) return;
        try {
            Path file = reticulum.getStoragePath().resolve(KNOWN_PEERS_FILE);
            // Prefer confirmed-active hashes; fall back to loaded hashes only if nothing was
            // confirmed this session (e.g., very short startup before any peer became ACTIVE).
            Set<String> toSave = knownPeerHashes.isEmpty() ? loadedPeerHashes : knownPeerHashes;
            Files.write(file, toSave, UTF_8);
            log.debug("Saved {} known peer hashes to {}", toSave.size(), file);
        } catch (IOException e) {
            log.warn("Failed to save known peer hashes: {}", e.getMessage());
        }
    }

    // Called from ReticulumPeer.createPeerBuffer() when a peer's buffer is confirmed ACTIVE.
    // Only initiator peers call this (non-initiators have our own destination hash, not the remote's).
    void confirmPeerHash(String hashHex, RNSCommon.PeerAspect aspect) {
        // Peer is ACTIVE — clear any failure/backoff state so a future transient drop starts fresh
        // rather than inheriting a long exponential-backoff window from earlier.
        clearPendingFailure(hashHex);
        if (aspect == RNSCommon.PeerAspect.DATA) {
            boolean isNew = this.knownDataPeerHashes.add(hashHex);
            if (isNew) {
                saveKnownDataPeerHashes();
                log.debug("Confirmed ACTIVE DATA peer hash {}", hashHex);
            }
        } else {
            boolean isNew = this.knownPeerHashes.add(hashHex);
            if (isNew) {
                saveKnownPeerHashes();
                log.debug("Confirmed ACTIVE peer hash {}", hashHex);
            }
        }
    }

    private void loadKnownPeerHashes() {
        if (reticulum == null) return;
        try {
            Path file = reticulum.getStoragePath().resolve(KNOWN_PEERS_FILE);
            if (!Files.isReadable(file)) return;
            List<String> lines = Files.readAllLines(file, UTF_8);
            int loaded = 0;
            for (String line : lines) {
                String hex = line.trim();
                if (!hex.isEmpty()) {
                    loadedPeerHashes.add(hex); // loaded into separate set; confirmed-active entries go to knownPeerHashes
                    loaded++;
                }
            }
            if (loaded > 0) {
                log.info("Loaded {} known peer hashes from {}", loaded, file);
            }
        } catch (IOException e) {
            log.warn("Failed to load known peer hashes: {}", e.getMessage());
        }
    }

    private void saveKnownDataPeerHashes() {
        if (reticulum == null) return;
        try {
            Path file = reticulum.getStoragePath().resolve(KNOWN_DATA_PEERS_FILE);
            Set<String> toSave = knownDataPeerHashes.isEmpty() ? loadedDataPeerHashes : knownDataPeerHashes;
            Files.write(file, toSave, UTF_8);
            log.debug("Saved {} known DATA peer hashes to {}", toSave.size(), file);
        } catch (IOException e) {
            log.warn("Failed to save known DATA peer hashes: {}", e.getMessage());
        }
    }

    private void loadKnownDataPeerHashes() {
        if (reticulum == null) return;
        try {
            Path file = reticulum.getStoragePath().resolve(KNOWN_DATA_PEERS_FILE);
            if (!Files.isReadable(file)) return;
            List<String> lines = Files.readAllLines(file, UTF_8);
            int loaded = 0;
            for (String line : lines) {
                String hex = line.trim();
                if (!hex.isEmpty()) {
                    loadedDataPeerHashes.add(hex);
                    loaded++;
                }
            }
            if (loaded > 0) {
                log.info("Loaded {} known DATA peer hashes from {}", loaded, file);
            }
        } catch (IOException e) {
            log.warn("Failed to load known DATA peer hashes: {}", e.getMessage());
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

