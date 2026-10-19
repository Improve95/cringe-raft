package ru.nsu.raft.node;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import ru.nsu.raft.config.ClusterProperties;
import ru.nsu.raft.config.ClusterProperties.Peer;
import ru.nsu.raft.node.event.RaftEvent;
import ru.nsu.raft.node.state.*;
import ru.nsu.raft.rpc.*;
import ru.nsu.raft.runtime.*;
import ru.nsu.raft.transport.PeerTransport;

// Все методы, кроме snapshot(), вызываются исключительно потоком EventLoop.
@Component
public class RaftNode {
    private static final Logger logger = LoggerFactory.getLogger(RaftNode.class);
    private final ClusterProperties properties;
    private final ElectionTimer electionTimer;
    private final HeartbeatScheduler heartbeat;
    private final PeerTransport transport;
    private final RaftLog log = new RaftLog();
    private long currentTerm;
    private String votedFor;
    private String leaderId;
    private RaftState state;
    private volatile NodeSnapshot snapshot;

    public RaftNode(ClusterProperties properties, ElectionTimer electionTimer,
            HeartbeatScheduler heartbeat, PeerTransport transport) {
        this.properties = properties;
        this.electionTimer = electionTimer;
        this.heartbeat = heartbeat;
        this.transport = transport;
        publish();
    }
    public void handle(RaftEvent event) {
        try {
            switch (event) {
                case RaftEvent.NodeStarted ignored -> becomeFollower(currentTerm);
                case RaftEvent.ElectionTimeout timeout -> {
                    if (electionTimer.isCurrent(timeout.epoch())) state.onElectionTimeout();
                }
                case RaftEvent.HeartbeatTick ignored -> state.onHeartbeatTick();
                case RaftEvent.RequestVoteReceived rpc -> rpc.reply().complete(state.onRequestVote(rpc.req()));
                case RaftEvent.AppendEntriesReceived rpc -> rpc.reply().complete(state.onAppendEntries(rpc.req()));
                case RaftEvent.VoteResponseReceived rpc -> state.onVoteResponse(rpc.from(), rpc.resp());
                case RaftEvent.AppendEntriesResponseReceived rpc -> state.onAppendEntriesResponse(rpc);
                case RaftEvent.AddEntryReceived command -> state.onAddEntry(command.command(), command.reply());
            }
        } finally { publish(); }
    }
    public void becomeFollower(long term) {
        if (term > currentTerm) {
            currentTerm = term;
            votedFor = null;
        }
        leaderId = null;
        transitionTo(new FollowerState(this));
    }
    public void becomeCandidate() { transitionTo(new CandidateState(this)); }
    public void becomeLeader() { transitionTo(new LeaderState(this)); }
    private void transitionTo(RaftState next) {
        var previous = state;
        if (previous != null) previous.onExit();
        state = next;
        logger.info("[{}] term={} {} -> {}", selfId(), currentTerm,
                previous == null ? "START" : previous.role(), next.role());
        next.onEnter();
    }
    private void publish() {
        var status = new NodeStatus(selfId(), state == null ? NodeRole.FOLLOWER : state.role(), currentTerm, leaderId);
        snapshot = new NodeSnapshot(status, log.snapshot());
    }
    public NodeSnapshot snapshot() { return snapshot; }
    public long term() { return currentTerm; }
    public String selfId() { return properties.nodeId(); }
    public String leaderId() { return leaderId; }
    public RaftState state() { return state; }
    public RaftLog log() { return log; }
    public List<Peer> peers() { return properties.peers(); }
    public int clusterSize() { return peers().size() + 1; }
    public int quorum() { return clusterSize() / 2 + 1; }
    public boolean isPeer(String id) { return peers().stream().anyMatch(peer -> peer.id().equals(id)); }
    public void incrementTerm() { currentTerm++; votedFor = null; }
    public boolean canVoteFor(String id) { return votedFor == null || votedFor.equals(id); }
    public void recordVote(String id) { votedFor = id; }
    public void setLeader(String id) { leaderId = id; }
    public void resetElectionTimer() { electionTimer.reset(); }
    public void cancelElectionTimer() { electionTimer.cancel(); }
    public void startHeartbeat() { heartbeat.start(); }
    public void stopHeartbeat() { heartbeat.stop(); }
    public void broadcast(RequestVoteRequest request) { peers().forEach(peer -> transport.requestVote(peer, request)); }
    public void sendAppend(String id, long sequence, AppendEntriesRequest request) {
        peers().stream().filter(peer -> peer.id().equals(id)).findFirst()
                .ifPresent(peer -> transport.appendEntries(peer, sequence, request));
    }
}
