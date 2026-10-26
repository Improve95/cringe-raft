package ru.nsu.raft.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.env.SystemEnvironmentPropertySource;
import ru.nsu.raft.node.NodeRole;
import ru.nsu.raft.node.NodeStatus;
import ru.nsu.raft.node.RaftNode;
import ru.nsu.raft.node.event.ElectionTimeout;
import ru.nsu.raft.node.event.NodeStarted;
import ru.nsu.raft.rpc.RequestVoteRequest;
import ru.nsu.raft.runtime.ElectionTimer;
import ru.nsu.raft.runtime.HeartbeatScheduler;
import ru.nsu.raft.transport.PeerClient;

import java.net.URI;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class ClusterEnvironmentTest {
    @Test
    void bindsComposeEnvironmentAndUsesItForElections() {
        var environment = Map.<String, Object>of(
            "RAFT_NODEID", "node-2",
            "RAFT_PEERS_0_ID", "node-1",
            "RAFT_PEERS_0_URL", "http://node-1:8080",
            "RAFT_PEERS_1_ID", "node-3",
            "RAFT_PEERS_1_URL", "http://node-3:8080");
        var timer = mock(ElectionTimer.class);
        var client = mock(PeerClient.class);
        when(timer.isCurrent(1)).thenReturn(true);

        new ApplicationContextRunner()
            .withInitializer(context -> context.getEnvironment().getPropertySources()
                .addFirst(new SystemEnvironmentPropertySource("systemEnvironment", environment)))
            .withInitializer(new ConfigDataApplicationContextInitializer())
            .withUserConfiguration(RaftNodeConfiguration.class)
            .withBean(ElectionTimer.class, () -> timer)
            .withBean(HeartbeatScheduler.class, () -> mock(HeartbeatScheduler.class))
            .withBean(PeerClient.class, () -> client)
            .run(context -> {
                assertThat(context).hasNotFailed();
                var properties = context.getBean(ClusterProperties.class);
                assertThat(properties.nodeId()).isEqualTo("node-2");
                assertThat(properties.peers()).containsExactly(
                    new ClusterProperties.Peer("node-1", URI.create("http://node-1:8080")),
                    new ClusterProperties.Peer("node-3", URI.create("http://node-3:8080")));
                assertThat(properties.clusterSize()).isEqualTo(3);

                var node = context.getBean(RaftNode.class);
                node.handle(new NodeStarted());
                node.handle(new ElectionTimeout(1));
                assertThat(node.snapshot().status())
                    .isEqualTo(new NodeStatus("node-2", NodeRole.CANDIDATE, 1, null));
                var request = new RequestVoteRequest(1, "node-2", 0, 0);
                verify(client).requestVote(properties.peers().get(0), request);
                verify(client).requestVote(properties.peers().get(1), request);
                verifyNoMoreInteractions(client);
            });
    }
}
