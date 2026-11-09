package ru.nsu.raft.config;

import org.junit.jupiter.api.Test;
import ru.nsu.raft.node.RaftNode;
import ru.nsu.raft.node.event.HeartbeatTick;
import ru.nsu.raft.node.event.RaftEvent;
import ru.nsu.raft.support.NodeFixture;

import java.util.HashMap;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RaftEventConfigurationTest {
    @Test
    void registersEveryPermittedEvent() {
        var handlers = new RaftEventConfiguration().raftEventHandlers();
        Set<Class<?>> eventTypes = Set.of(RaftEvent.class.getPermittedSubclasses());
        assertThat(handlers.keySet()).isEqualTo(eventTypes);
        assertThatThrownBy(handlers::clear).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void dispatchesThroughConfiguredHandlerAndPublishesSnapshotEvenOnFailure() {
        var fixture = new NodeFixture();
        var handlers = new HashMap<>(new RaftEventConfiguration().raftEventHandlers());
        handlers.put(
            HeartbeatTick.class, (node, event) -> {
                node.incrementTerm();
                throw new IllegalStateException("handler failed");
            });
        var node = new RaftNode(
            fixture.node.properties(), fixture.timer, fixture.heartbeat, fixture.client, handlers);
        handlers.clear();

        assertThatThrownBy(() -> node.handle(new HeartbeatTick()))
            .isInstanceOf(IllegalStateException.class).hasMessage("handler failed");
        assertThat(node.nodeStatus().term()).isEqualTo(1);
    }
}
