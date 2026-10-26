package ru.nsu.raft.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.nsu.raft.node.RaftNode;
import ru.nsu.raft.node.logger.RaftLog;
import ru.nsu.raft.runtime.ElectionTimer;
import ru.nsu.raft.runtime.HeartbeatScheduler;
import ru.nsu.raft.transport.PeerClient;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(ClusterProperties.class)
public class RaftNodeConfiguration {
    @Bean
    RaftLog raftLog() {
        return new RaftLog();
    }

    @Bean
    RaftNode raftNode(
        ClusterProperties properties, RaftLog log, ElectionTimer electionTimer,
        HeartbeatScheduler heartbeatScheduler, PeerClient peerClient
    ) {
        return new RaftNode(properties, log, electionTimer, heartbeatScheduler, peerClient);
    }
}
