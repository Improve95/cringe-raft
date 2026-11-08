package ru.nsu.raft.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@ConfigurationProperties("raft")
public record ClusterProperties(
    @DefaultValue("node-1") String nodeId,
    Map<String, URI> nodes,
    @DefaultValue("300ms") Duration electionTimeoutMin,
    @DefaultValue("600ms") Duration electionTimeoutMax,
    @DefaultValue("75ms") Duration heartbeatInterval,
    @DefaultValue("1s") Duration rpcTimeout
) {

    public ClusterProperties {
        if (nodeId == null || nodeId.isBlank()) {
            throw new IllegalArgumentException("raft.node-id must not be blank");
        }
        validateNodes(nodeId, nodes);
        nodes = Collections.unmodifiableMap(new LinkedHashMap<>(nodes));
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
    }

    static void requirePositive(Duration duration, String name) {
        if (duration == null || duration.toMillis() < 1) {
            throw new IllegalArgumentException(name + " must be at least 1ms");
        }
    }

    private static void validateNodes(String nodeId, Map<String, URI> nodes) {
        if (nodes == null || nodes.isEmpty()) {
            throw new IllegalArgumentException("raft.nodes must contain at least one node");
        }
        nodes.forEach(Peer::new);
        if (!nodes.containsKey(nodeId)) {
            throw new IllegalArgumentException("raft.node-id is absent from raft.nodes: " + nodeId);
        }
    }

    public List<Peer> peers() {
        return nodes.entrySet().stream()
            .filter(entry -> !entry.getKey().equals(nodeId))
            .map(entry -> new Peer(entry.getKey(), entry.getValue()))
            .toList();
    }

    public int clusterSize() {
        return nodes.size();
    }

    public int quorum() {
        return clusterSize() / 2 + 1;
    }

    public boolean hasPeer(String id) {
        return !nodeId.equals(id) && nodes.containsKey(id);
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
