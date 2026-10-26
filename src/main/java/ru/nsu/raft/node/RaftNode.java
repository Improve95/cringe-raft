package ru.nsu.raft.node;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.nsu.raft.config.ClusterProperties;
import ru.nsu.raft.node.event.AppendEntriesReceived;
import ru.nsu.raft.node.event.AppendEntriesResponseReceived;
import ru.nsu.raft.node.event.ClientEntryReceived;
import ru.nsu.raft.node.event.ElectionTimeout;
import ru.nsu.raft.node.event.HeartbeatTick;
import ru.nsu.raft.node.event.NodeStarted;
import ru.nsu.raft.node.event.RaftEvent;
import ru.nsu.raft.node.event.RequestVoteReceived;
import ru.nsu.raft.node.event.VoteResponseReceived;
import ru.nsu.raft.node.logger.RaftLog;
import ru.nsu.raft.node.state.CandidateState;
import ru.nsu.raft.node.state.FollowerState;
import ru.nsu.raft.node.state.LeaderState;
import ru.nsu.raft.node.state.RaftState;
import ru.nsu.raft.runtime.ElectionTimer;
import ru.nsu.raft.runtime.HeartbeatScheduler;
import ru.nsu.raft.transport.PeerClient;

// Контекст намеренно не потокобезопасен: к нему обращается только EventLoop.
public final class RaftNode {
    private static final Logger log = LoggerFactory.getLogger(RaftNode.class);
    private final ClusterProperties properties;
    private final RaftLog raftLog;
    private final ElectionTimer electionTimer;
    private final HeartbeatScheduler heartbeatScheduler;
    private final PeerClient peerClient;
    private long currentTerm;
    private String votedFor;
    private String leaderId;
    private RaftState state;
    private volatile NodeSnapshot snapshot;

    public RaftNode(
        ClusterProperties properties,
        RaftLog raftLog,
        ElectionTimer electionTimer,
        HeartbeatScheduler heartbeatScheduler,
        PeerClient peerClient
    ) {
        this.properties = properties;
        this.raftLog = raftLog;
        this.electionTimer = electionTimer;
        this.heartbeatScheduler = heartbeatScheduler;
        this.peerClient = peerClient;
        state = new FollowerState(this);
        publishSnapshot();
    }

    public void handle(RaftEvent event) {
        try {
            switch (event) {
                case NodeStarted ignored -> state.onEnter();
                case ElectionTimeout timeout -> handleElectionTimeout(timeout);
                case HeartbeatTick ignored -> state.onHeartbeatTick();
                case RequestVoteReceived vote -> vote.reply().complete(state.onRequestVote(vote.request()));
                case AppendEntriesReceived append -> append.reply().complete(state.onAppendEntries(append.request()));
                case VoteResponseReceived vote -> state.onVoteResponse(vote.from(), vote.response());
                case AppendEntriesResponseReceived append -> state.onAppendEntriesResponse(append);
                case ClientEntryReceived entry -> state.onClientEntry(entry);
            }
        } finally {
            publishSnapshot();
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

    public NodeSnapshot snapshot() {
        return snapshot;
    }

    public long term() {
        return currentTerm;
    }

    public String leaderId() {
        return leaderId;
    }

    public ClusterProperties properties() {
        return properties;
    }

    public RaftLog log() {
        return raftLog;
    }

    public ElectionTimer electionTimer() {
        return electionTimer;
    }

    public HeartbeatScheduler heartbeatScheduler() {
        return heartbeatScheduler;
    }

    public PeerClient peerClient() {
        return peerClient;
    }

    public RaftState state() {
        return state;
    }

    public void stop() {
        state.onExit();
        electionTimer.cancel();
    }

    private void handleElectionTimeout(ElectionTimeout timeout) {
        if (electionTimer.isCurrent(timeout.epoch())) {
            state.onElectionTimeout();
        }
    }

    private void transitionTo(RaftState next) {
        state.onExit();
        log.info("[{}] term={} {} -> {}", properties.nodeId(), currentTerm, state.role(), next.role());
        state = next;
        state.onEnter();
    }

    private void publishSnapshot() {
        var status = new NodeStatus(properties.nodeId(), state.role(), currentTerm, leaderId);
        snapshot = new NodeSnapshot(status, raftLog.snapshot());
    }
}
