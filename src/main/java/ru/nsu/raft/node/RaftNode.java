package ru.nsu.raft.node;

import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import ru.nsu.raft.config.ClusterProperties;
import ru.nsu.raft.node.event.RaftEvent;
import ru.nsu.raft.node.handler.RaftEventHandler;
import ru.nsu.raft.node.state.CandidateState;
import ru.nsu.raft.node.state.FollowerState;
import ru.nsu.raft.node.state.LeaderState;
import ru.nsu.raft.node.state.RaftState;
import ru.nsu.raft.runtime.ElectionTimer;
import ru.nsu.raft.runtime.HeartbeatScheduler;
import ru.nsu.raft.transport.PeerClient;

import java.util.Map;

// Контекст намеренно не потокобезопасен: к нему обращается только EventLoop.
@Slf4j
@Accessors(fluent = true)
public final class RaftNode {
    @Getter
    private final ClusterProperties properties;
    @Getter
    private final ElectionTimer electionTimer;
    @Getter
    private final HeartbeatScheduler heartbeatScheduler;
    @Getter
    private final PeerClient peerClient;
    private final Map<Class<? extends RaftEvent>, RaftEventHandler<RaftEvent>> eventHandlers;
    private long currentTerm;
    private String votedFor;
    @Getter
    private String leaderId;
    @Getter
    private RaftState state;
    @Getter
    private volatile NodeStatus nodeStatus;

    public RaftNode(
        ClusterProperties properties,
        ElectionTimer electionTimer,
        HeartbeatScheduler heartbeatScheduler,
        PeerClient peerClient,
        Map<Class<? extends RaftEvent>, RaftEventHandler<RaftEvent>> eventHandlers
    ) {
        this.properties = properties;
        this.electionTimer = electionTimer;
        this.heartbeatScheduler = heartbeatScheduler;
        this.peerClient = peerClient;
        this.eventHandlers = Map.copyOf(eventHandlers);
        state = new FollowerState(this);
        publishNodeStatus();
    }

    public void handle(RaftEvent event) {
        try {
            var handler = eventHandlers.get(event.getClass());
            if (handler == null) {
                throw new IllegalArgumentException("No handler for event: " + event.getClass().getSimpleName());
            }
            handler.handle(this, event);
        } finally {
            publishNodeStatus();
        }
    }

    public void becomeFollower(long term) {
        if (term > currentTerm) {
            currentTerm = term;
            votedFor = null;
        }
        leaderId = null;
        transitionTo(new FollowerState(this));
    }

    public void becomeCandidate() {
        transitionTo(new CandidateState(this));
    }

    public void becomeLeader() {
        transitionTo(new LeaderState(this));
    }

    public void incrementTerm() {
        currentTerm++;
        votedFor = null;
        leaderId = null;
    }

    public boolean canVoteFor(String candidateId) {
        return votedFor == null || votedFor.equals(candidateId);
    }

    public void recordVote(String candidateId) {
        votedFor = candidateId;
        log.info("[{}] term={} voted for {}", properties.nodeId(), currentTerm, candidateId);
    }

    public void setLeader(String id) {
        leaderId = id;
    }

    public long term() {
        return currentTerm;
    }

    public void stop() {
        state.onExit();
        electionTimer.cancel();
    }

    private void transitionTo(RaftState next) {
        state.onExit();
        log.info("[{}] term={} {} -> {}", properties.nodeId(), currentTerm, state.role(), next.role());
        state = next;
        state.onEnter();
    }

    private void publishNodeStatus() {
        nodeStatus = new NodeStatus(properties.nodeId(), state.role(), currentTerm, leaderId);
    }
}
