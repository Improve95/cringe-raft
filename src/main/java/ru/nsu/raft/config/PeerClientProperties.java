package ru.nsu.raft.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties("raft.transport")
public record PeerClientProperties(
        @DefaultValue("1s") Duration connectTimeout,
        @DefaultValue("1s") Duration readTimeout) {
    public PeerClientProperties {
        if (connectTimeout == null || connectTimeout.toMillis() <= 0
                || readTimeout == null || readTimeout.toMillis() <= 0) {
            throw new IllegalArgumentException("Raft transport timeouts must be positive");
        }
    }
}
