package ru.nsu.raft.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.net.URI;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConfigurationTest {
    @Test
    void rejectsDuplicateAndSelfPeerIdsAndInvalidTimeouts() {
        var peer = new ClusterProperties.Peer("b", URI.create("http://localhost:8082"));
        assertThatThrownBy(() -> properties(List.of(peer, peer), 50, 300, 600))
            .isInstanceOf(IllegalArgumentException.class);
        var self = new ClusterProperties.Peer("a", URI.create("http://localhost:8081"));
        assertThatThrownBy(() -> properties(List.of(self), 50, 300, 600))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> properties(List.of(), 300, 300, 600))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> properties(List.of(), 50, 600, 300))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void invalidTransportTimeoutFailsDuringConfigurationBinding() {
        new ApplicationContextRunner().withUserConfiguration(PeerClientConfiguration.class)
            .withBean(ru.nsu.raft.runtime.EventQueue.class)
            .withPropertyValues("raft.transport.read-timeout=0ms")
            .run(context -> assertThat(context).hasFailed());
    }

    private ClusterProperties properties(List<ClusterProperties.Peer> peers, long heartbeat, long min, long max) {
        return new ClusterProperties(
            "a", peers, Duration.ofMillis(min), Duration.ofMillis(max),
            Duration.ofMillis(heartbeat), Duration.ofSeconds(1));
    }
}
