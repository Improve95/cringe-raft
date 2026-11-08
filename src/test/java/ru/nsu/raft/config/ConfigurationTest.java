package ru.nsu.raft.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.net.URI;
import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConfigurationTest {
    @Test
    void rejectsEmptyClusterUnknownNodeAndInvalidTimeouts() {
        var nodes = Map.of("a", URI.create("http://localhost:8081"));
        assertThatThrownBy(() -> properties(Map.of(), 50, 300, 600))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> properties(Map.of("b", URI.create("http://localhost:8082")), 50, 300, 600))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> properties(nodes, 300, 300, 600))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> properties(nodes, 50, 600, 300))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void invalidTransportTimeoutFailsDuringConfigurationBinding() {
        new ApplicationContextRunner().withUserConfiguration(PeerClientConfiguration.class)
            .withBean(ru.nsu.raft.runtime.EventQueue.class)
            .withPropertyValues("raft.transport.read-timeout=0ms")
            .run(context -> assertThat(context).hasFailed());
    }

    private ClusterProperties properties(Map<String, URI> nodes, long heartbeat, long min, long max) {
        return new ClusterProperties(
            "a", nodes, Duration.ofMillis(min), Duration.ofMillis(max),
            Duration.ofMillis(heartbeat), Duration.ofSeconds(1));
    }
}
