package org.qortal.network.reticulum;

import io.reticulum.Reticulum;
import io.reticulum.Transport;
import io.reticulum.interfaces.ConnectionInterface;
import io.reticulum.destination.Destination;
import io.reticulum.destination.DestinationType;
import io.reticulum.destination.Direction;
import io.reticulum.destination.ProofStrategy;
import io.reticulum.identity.Identity;
import io.reticulum.link.Link;
import io.reticulum.transport.AnnounceHandler;
import static io.reticulum.link.LinkStatus.ACTIVE;
import static io.reticulum.link.LinkStatus.CLOSED;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.commons.codec.binary.Hex.encodeHexString;
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

    // Per-peer link-failure state and its capped exponential backoff — see ReconnectPolicy.
    // One instance per aspect: BASE and DATA destination hashes are distinct, so a hash can only
    // ever appear in one of them.
    private final ReconnectPolicy basePolicy = new ReconnectPolicy();
    private final ReconnectPolicy dataPolicy = new ReconnectPolicy();

    /** Owns the linked (initiator) and incoming (non-initiator) peer lists and their snapshots. */
    private final RNSPeerRegistry registry = new RNSPeerRegistry();

    /** The four prunePeers() passes. Removal side effects stay here, behind the two callbacks. */
    private final RNSPeerPruner peerPruner = new RNSPeerPruner(
            registry, this::removeLinkedPeer, this::removeIncomingPeer,
            (hashHex, aspect) -> policyFor(aspect).recordFailure(hashHex));

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

    /** One mesh loop per aspect: drain, announce, reconnect. Both created in start(). */
    private RNSAspectRunner baseRunner;
    private RNSAspectRunner dataRunner;
    /** Thread priority for the runners' executors; the constructor reads it from Settings. */
    private final int rnsThreadPriority;
    /** Shared by both runners: peer message and ping tasks from either aspect run here. */
    private ExecutorService rnsWorkerPool;
    private static final long NETWORK_EPC_KEEPALIVE = 5L; // 1 second

    // replicating a feature from Network.class needed in for base Message.java,
    // just in case the classic TCP/IP Networking is turned off.
    private static final byte[] MAINNET_MESSAGE_MAGIC = new byte[]{0x51, 0x4f, 0x52, 0x54}; // QORT
    private static final byte[] TESTNET_MESSAGE_MAGIC = new byte[]{0x71, 0x6f, 0x72, 0x54}; // qort

    /** The failure/backoff state for an aspect. */
    private ReconnectPolicy policyFor(RNSCommon.PeerAspect aspect) {
        return aspect == RNSCommon.PeerAspect.DATA ? dataPolicy : basePolicy;
    }

    /** Called by ReticulumPeer.linkClosed() to kick the announce/path-recovery cycle soon. */
    public void triggerImmediateAnnounce() {
        if (baseRunner != null) {   // null until start() has run
            baseRunner.triggerImmediateAnnounce();
        }
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
        this.rnsThreadPriority = rnsThreadPriority;
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
        // Start up "main" threads, one per destination / peer aspect. Each runner seeds its own
        // announce timer from its peer store (full window on a first-ever start, 15s on a restart
        // with known hashes to reconnect to).
        //
        // Only BASE logs per-interface online status: interface state is transport-wide, not
        // per-aspect, so logging it from both runners would just double the line rate.
        this.baseRunner = newRunner(RNSCommon.PeerAspect.BASE, baseDestination, MIN_DESIRED_CORE_PEERS,
                Peer.NETWORK, basePeerStore, basePolicy, true);
        this.dataRunner = newRunner(RNSCommon.PeerAspect.DATA, dataDestination, MIN_DESIRED_DATA_PEERS,
                Peer.NETWORKDATA, dataPeerStore, dataPolicy, false);
        this.baseRunner.start();
        this.dataRunner.start();

        this.meshStarted = true;
        log.info("RNS mesh started, baseDestination: {}", encodeHexString(baseDestination.getHash()));
    }

    /** One runner per aspect; everything that differs between the two is an argument here. */
    private RNSAspectRunner newRunner(RNSCommon.PeerAspect aspect, Destination destination,
                                      int minDesiredPeers, int messageTaskType, KnownPeerStore store,
                                      ReconnectPolicy policy, boolean logInterfaceStatus) {
        return new RNSAspectRunner(aspect, destination, minDesiredPeers, messageTaskType, store,
                registry, policy, gatewayManager, rnsWorkerPool, this::buildAnnounceAppData,
                (dhash, identity) -> createLinkedPeerFromIdentity(dhash, identity, aspect),
                this::isShuttingDown, logInterfaceStatus, rnsThreadPriority);
    }

    public boolean isMeshStarted() {
        return meshStarted;
    }

    /** Kick the DATA announce/reconnect cycle within ~5s (mirrors triggerImmediateAnnounce()). */
    public void triggerImmediateDataAnnounce() {
        if (dataRunner != null) {   // null until start() has run
            dataRunner.triggerImmediateAnnounce();
        }
    }

    public void broadcast(Function<ReticulumPeer, Message> peerMessageBuilder) {
        List<ReticulumPeer> allPeers = Stream.concat(
                registry.activeLinked().stream(),
                registry.activeIncoming().stream()
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

        // Stops each loop thread and its announce/reconnect executors together, before the peer
        // teardown below — a reconnect task must not create fresh links while we close them.
        if (this.baseRunner != null) {
            this.baseRunner.shutdown();
        }
        if (this.dataRunner != null) {
            this.dataRunner.shutdown();
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
        this.gatewayManager.shutdown();
        try {
            if (!this.rnsWorkerPool.awaitTermination(2, TimeUnit.SECONDS))
                this.rnsWorkerPool.shutdownNow();
        } catch (InterruptedException e) {
            this.rnsWorkerPool.shutdownNow();
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
        log.info("Data Client connected, data link: {}", encodeHexString(link.getLinkId()));
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
                        // DEBUG, not INFO: this whole loop runs per received announce, and every
                        // peer on the mesh announces every ~30s. Only the "added new peer" line
                        // below is a state change worth an INFO.
                        log.debug("QAnnounceHandler - peer exists - found peer matching destinationHash");
                        if (nonNull(p.getPeerLink())) {
                            log.debug("peer link: {}, status: {}",
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
                            log.debug("QAnnounceHandler - peer link is null");
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

    // Create and add an initiator ReticulumPeer directly from a cached identity (no announce
    // needed). Called from a runner's reconnect pass when recall() finds the identity in the local
    // known-destinations DB.
    //
    // The ReticulumPeer constructor calls initPeerLink() (which sends the LINK OPEN via outbound()).
    // Do NOT call getOrInitPeerLink() here: the peer's link is already PENDING, so getOrInitPeerLink()
    // would call initPeerLink() a second time — creating a zombie PENDING link in the Reticulum library.
    // The zombie establishes on the remote end (adding a spurious incoming peer there), and when it
    // times out it fires expirePath() → tablesLastCulled=EPOCH → cascading 60-120s cull cycles.
    private void createLinkedPeerFromIdentity(byte[] destinationHash, Identity identity,
                                              RNSCommon.PeerAspect aspect) {
        ReticulumPeer newPeer = new ReticulumPeer(destinationHash, aspect);
        newPeer.setServerIdentity(identity);
        newPeer.setIsInitiator(true);
        newPeer.setMessageMagic(getMessageMagic());
        addLinkedPeer(newPeer);
        log.info("{}: proactively connecting to known peer {} via cached identity",
                aspect, encodeHexString(destinationHash));
        // Link already created in constructor — do NOT call getOrInitPeerLink() here.
        // Detect immediate send failure: ReticulumPeer() → initPeerLink() → new Link() → packet.send()
        // → outbound() is synchronous; if the LINKREQUEST couldn't be sent (no route, backbone down),
        // the link is already CLOSED by the time we get here. Record a failure so the reconnect loop
        // backs off to requestPath() rather than creating a new CLOSED link on every 15s cycle.
        Link lnk = newPeer.getPeerLink();
        if (lnk != null && lnk.getStatus() == CLOSED) {
            log.warn("{}: LINKREQUEST to {} failed immediately — switching to requestPath backoff",
                    aspect, encodeHexString(destinationHash));
            policyFor(aspect).recordFailure(encodeHexString(destinationHash));
        }
    }

    private static class SingletonContainer {
        private static final RNS INSTANCE = new RNS();
    }

    public static RNS getInstance() {
        return SingletonContainer.INSTANCE;
    }

    public List<ReticulumPeer> getImmutableLinkedPeers() {
        return registry.linked();
    }

    public List<ReticulumPeer> getImmutableIncomingPeers() {
        return registry.incoming();
    }

    public List<ReticulumPeer> getActiveImmutableLinkedPeers() {
        return registry.activeLinked();
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
        // The registry dedups atomically with the add: receivedAnnounce() and the reconnect task
        // can both call this concurrently when a peer drops and reconnects.
        if (!registry.addLinked(peer)) {
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
        // Hash is persisted only once the peer's buffer is confirmed ACTIVE (see confirmPeerHash(),
        // called from ReticulumPeer.createPeerBuffer()). This prevents transient/failed connections
        // from accumulating in the persisted peer list.
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
        registry.removeLinked(peer);
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
        // The registry evicts any existing incoming peer from the same node with the same aspect
        // (identity + aspect, so a remote's CORE and DATA peers don't evict each other) and hands
        // the superseded ones back for teardown out here, with no registry lock held.
        for (ReticulumPeer superseded : registry.addIncoming(peer)) {
            superseded.shutdownChannel();
            // The superseded peer always holds a different Link object than the replacement (both
            // baseClientConnected and dataClientConnected build a fresh ReticulumPeer per incoming
            // Link), so closing it cannot disturb the new peer. Without this its watchdog thread
            // leaks (see removeLinkedPeer).
            closeIfActive(superseded);
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
        if (keep.getServerIdentity() == null) return;
        try {
            rnsWorkerPool.submit(() -> {
                for (ReticulumPeer p : registry.duplicateIncomingByIdentity(keep)) {
                    log.info("dedupIncomingPeerByIdentity: removing duplicate {} incoming peer from {}",
                            p.getPeerAspect(), RNSPeerRegistry.incomingIdentityKey(p));
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
        registry.removeIncoming(peer);
        // Incoming BASE peers are also registered in Network's connected/handshaked lists via
        // makePeerAvailable(); remove them here so they don't leak (see removeLinkedPeer).
        peer.makePeerUnavailable();
    }

    /** Whether a peer's link is dead, marked for removal, or has gone silent. */
    public boolean isUnreachable(ReticulumPeer peer) {
        return RNSPeerPruner.isUnreachable(peer);
    }

    /**
     * Periodic peer-list garbage collection, called from Controller every 90 seconds.
     * <p>
     * Keeps {@code throws DataException} because Controller.java wraps the call in a
     * {@code catch (DataException)} — Java rejects a catch clause for a checked exception the body
     * cannot throw, so dropping it here would force an unrelated Controller edit.
     */
    public void prunePeers() throws DataException {
        peerPruner.prune();
    }

    // Called from ReticulumPeer.createPeerBuffer() when a peer's buffer is confirmed ACTIVE.
    // Only initiator peers call this (non-initiators have our own destination hash, not the remote's).
    void confirmPeerHash(String hashHex, RNSCommon.PeerAspect aspect) {
        // Peer is ACTIVE — clear any failure/backoff state so a future transient drop starts fresh
        // rather than inheriting a long exponential-backoff window from earlier.
        policyFor(aspect).clear(hashHex);
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
                registry.activeLinked(RNSCommon.PeerAspect.DATA).stream(),
                registry.activeIncoming(RNSCommon.PeerAspect.DATA).stream()
        ).collect(Collectors.toList());
    }

    public byte[] getMessageMagic() {
        return Settings.getInstance().isTestNet() ? TESTNET_MESSAGE_MAGIC : MAINNET_MESSAGE_MAGIC;
    }

}

