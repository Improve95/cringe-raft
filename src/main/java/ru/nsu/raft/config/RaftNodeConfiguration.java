package ru.nsu.raft.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import ru.nsu.raft.node.RaftNode;
import ru.nsu.raft.node.event.RaftEvent;
import ru.nsu.raft.node.handler.RaftEventHandler;
import ru.nsu.raft.runtime.ElectionTimer;
import ru.nsu.raft.runtime.HeartbeatScheduler;
import ru.nsu.raft.transport.PeerClient;

import java.util.Map;

@Configuration(proxyBeanMethods = false)
@Import(RaftEventConfiguration.class)
@EnableConfigurationProperties(ClusterProperties.class)
public class RaftNodeConfiguration {

    @Bean
    public RaftNode raftNode(
        ClusterProperties properties,
        ElectionTimer electionTimer,
        HeartbeatScheduler heartbeatScheduler,
        PeerClient peerClient,
        @Qualifier("raftEventHandlers") Map<Class<? extends RaftEvent>, RaftEventHandler<RaftEvent>> eventHandlers
    ) {
        return new RaftNode(properties, electionTimer, heartbeatScheduler, peerClient, eventHandlers);
    }
}
