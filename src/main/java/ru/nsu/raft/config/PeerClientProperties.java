package ru.nsu.raft.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

@ConfigurationProperties("raft.transport")
public record PeerClientProperties(
    @DefaultValue("500ms") Duration connectTimeout,
    @DefaultValue("2s") Duration readTimeout) {

    public PeerClientProperties {
        ClusterProperties.requirePositive(connectTimeout, "connect-timeout");
        ClusterProperties.requirePositive(readTimeout, "read-timeout");
    }
}
