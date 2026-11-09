package ru.nsu.raft.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.nsu.raft.node.event.AppendEntriesReceived;
import ru.nsu.raft.node.event.AppendEntriesResponseReceived;
import ru.nsu.raft.node.event.ElectionTimeout;
import ru.nsu.raft.node.event.HeartbeatTick;
import ru.nsu.raft.node.event.NodeStarted;
import ru.nsu.raft.node.event.RaftEvent;
import ru.nsu.raft.node.event.RequestVoteReceived;
import ru.nsu.raft.node.event.VoteResponseReceived;
import ru.nsu.raft.node.handler.AppendEntriesReceivedHandler;
import ru.nsu.raft.node.handler.AppendEntriesResponseReceivedHandler;
import ru.nsu.raft.node.handler.ElectionTimeoutHandler;
import ru.nsu.raft.node.handler.HeartbeatTickHandler;
import ru.nsu.raft.node.handler.NodeStartedHandler;
import ru.nsu.raft.node.handler.RaftEventHandler;
import ru.nsu.raft.node.handler.RequestVoteReceivedHandler;
import ru.nsu.raft.node.handler.VoteResponseReceivedHandler;

import java.util.Map;

@Configuration(proxyBeanMethods = false)
public class RaftEventConfiguration {

    private static <E extends RaftEvent> Map.Entry<Class<? extends RaftEvent>, RaftEventHandler<RaftEvent>> handler(
        Class<E> eventType,
        RaftEventHandler<E> handler
    ) {
        return Map.entry(eventType, (node, event) -> handler.handle(node, eventType.cast(event)));
    }

    @Bean
    public Map<Class<? extends RaftEvent>, RaftEventHandler<RaftEvent>> raftEventHandlers() {
        return Map.ofEntries(
            handler(NodeStarted.class, new NodeStartedHandler()),
            handler(ElectionTimeout.class, new ElectionTimeoutHandler()),
            handler(HeartbeatTick.class, new HeartbeatTickHandler()),
            handler(RequestVoteReceived.class, new RequestVoteReceivedHandler()),
            handler(AppendEntriesReceived.class, new AppendEntriesReceivedHandler()),
            handler(VoteResponseReceived.class, new VoteResponseReceivedHandler()),
            handler(AppendEntriesResponseReceived.class, new AppendEntriesResponseReceivedHandler()));
    }
}
