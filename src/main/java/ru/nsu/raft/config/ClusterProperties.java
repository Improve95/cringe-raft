package ru.nsu.raft.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.net.URI;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;

@ConfigurationProperties("raft")
public record ClusterProperties(
    @DefaultValue("node-1") String nodeId,
    List<Peer> peers,
    @DefaultValue("300ms") Duration electionTimeoutMin,
    @DefaultValue("600ms") Duration electionTimeoutMax,
    @DefaultValue("75ms") Duration heartbeatInterval,
    @DefaultValue("1s") Duration rpcTimeout) {

    public ClusterProperties {
        if (nodeId == null || nodeId.isBlank()) {
            throw new IllegalArgumentException("raft.node-id must not be blank");
        }
        peers = peers == null ? List.of() : List.copyOf(peers);
        requirePositive(electionTimeoutMin, "election-timeout-min");
        requirePositive(electionTimeoutMax, "election-timeout-max");
        requirePositive(heartbeatInterval, "heartbeat-interval");
        requirePositive(rpcTimeout, "rpc-timeout");
        if (electionTimeoutMax.compareTo(electionTimeoutMin) < 0) {
            throw new IllegalArgumentException("Election timeout maximum must be >= minimum");
        }
        if (heartbeatInterval.compareTo(electionTimeoutMin) >= 0) {
            throw new IllegalArgumentException("Heartbeat interval must be < election timeout");
        }
        validatePeers(nodeId, peers);
    }

    static void requirePositive(Duration duration, String name) {
        if (duration == null || duration.toMillis() < 1) {
            throw new IllegalArgumentException(name + " must be at least 1ms");
        }
    }

    private static void validatePeers(String nodeId, List<Peer> peers) {
        var ids = new HashSet<String>();
        ids.add(nodeId);
        for (Peer peer : peers) {
            if (!ids.add(peer.id())) {
                throw new IllegalArgumentException("Duplicate node id: " + peer.id());
            }
        }
    }

    public int clusterSize() {
        return peers.size() + 1;
    }

    public int quorum() {
        return clusterSize() / 2 + 1;
    }

    public boolean hasPeer(String id) {
        return peers.stream().anyMatch(peer -> peer.id().equals(id));
    }

    public record Peer(String id, URI url) {
        public Peer {
            if (id == null || id.isBlank()) {
                throw new IllegalArgumentException("Peer id must not be blank");
            }
            if (url == null || url.getHost() == null
                || !("http".equals(url.getScheme()) || "https".equals(url.getScheme()))
                || url.getRawQuery() != null || url.getRawFragment() != null) {
                throw new IllegalArgumentException("Peer URL must be an absolute HTTP(S) URL");
            }
        }
    }
}
