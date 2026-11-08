package ru.nsu.raft.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.context.config.ConfigDataResourceNotFoundException;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.SystemEnvironmentPropertySource;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ClusterFileTest {
    @TempDir
    Path directory;

    @Test
    void loadsExternalMapAndExcludesSelf() throws IOException {
        Path file = writeConfig("""
            raft:
              nodes:
                alpha: http://10.0.0.1:9000
                beta: http://10.0.0.2:9000
            """);
        context(file, "beta").run(context -> {
            assertThat(context).hasNotFailed();
            var properties = context.getBean(ClusterProperties.class);
            assertThat(properties.nodes()).containsExactlyInAnyOrderEntriesOf(Map.of(
                "alpha", URI.create("http://10.0.0.1:9000"), "beta", URI.create("http://10.0.0.2:9000")));
            assertThat(properties.peers()).containsExactly(
                new ClusterProperties.Peer("alpha", URI.create("http://10.0.0.1:9000")));
            assertThat(properties.clusterSize()).isEqualTo(2);
            assertThat(properties.quorum()).isEqualTo(2);
            assertThatThrownBy(() -> properties.nodes().clear()).isInstanceOf(UnsupportedOperationException.class);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "raft:\n  nodes: {}",
        "raft:\n  nodes:\n    other: http://localhost:8080",
        "raft:\n  nodes:\n    node-1: ftp://localhost:8080",
        "raft:\n  nodes:\n    node-1: http://localhost:8080\n    node-1: http://localhost:8081"
    })
    void rejectsInvalidOrAmbiguousCluster(String yaml) throws IOException {
        context(writeConfig(yaml), "node-1").run(context -> assertThat(context).hasFailed());
    }

    @Test
    void rejectsMissingFile() {
        context(directory.resolve("missing.yaml"), "node-1").run(context -> {
            assertThat(context).hasFailed();
            assertThat(context.getStartupFailure()).isInstanceOf(ConfigDataResourceNotFoundException.class);
        });
    }

    private Path writeConfig(String yaml) throws IOException {
        return Files.writeString(directory.resolve("cluster.yaml"), yaml);
    }

    private ApplicationContextRunner context(Path file, String nodeId) {
        return new ApplicationContextRunner()
            .withInitializer(context -> context.getEnvironment().getPropertySources().addFirst(
                new SystemEnvironmentPropertySource("systemEnvironment",
                    Map.of("RAFT_CONFIG", file.toString(), "RAFT_NODEID", nodeId))))
            .withInitializer(new ConfigDataApplicationContextInitializer())
            .withUserConfiguration(BindingConfiguration.class);
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(ClusterProperties.class)
    static class BindingConfiguration {
    }
}
