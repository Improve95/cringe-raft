package ru.nsu.raft.config;

import java.time.Duration;
import java.net.URI;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties("raft")
public record ClusterProperties(
        @DefaultValue("node-1") String nodeId,
        List<Peer> peers,
        @DefaultValue("150ms") Duration electionTimeoutMin,
        @DefaultValue("300ms") Duration electionTimeoutMax,
        @DefaultValue("50ms") Duration heartbeatInterval,
        @DefaultValue("1s") Duration rpcTimeout
) {
    public ClusterProperties {
        peers = peers == null ? List.of() : List.copyOf(peers);
        if (nodeId == null || nodeId.isBlank()) throw new IllegalArgumentException("raft.node-id is required");
        if (electionTimeoutMin.toMillis() <= 0 || electionTimeoutMax.compareTo(electionTimeoutMin) < 0
                || heartbeatInterval.toMillis() <= 0 || heartbeatInterval.compareTo(electionTimeoutMin) >= 0
                || rpcTimeout.toMillis() <= 0) {
            throw new IllegalArgumentException("Invalid Raft timer configuration");
        }
        if (peers.stream().map(Peer::id).distinct().count() != peers.size()
                || peers.stream().anyMatch(peer -> peer.id().equals(nodeId))) {
            throw new IllegalArgumentException("Peer ids must be unique and exclude this node");
        }
    }
    public record Peer(String id, URI url) {
        public Peer {
            if (id == null || id.isBlank() || url == null || url.getHost() == null
                    || !("http".equals(url.getScheme()) || "https".equals(url.getScheme()))) {
                throw new IllegalArgumentException("Each peer needs an id and HTTP URL");
            }
        }
    }
}
