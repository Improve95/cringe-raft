package ru.nsu.raft.node.event;

public sealed interface RaftEvent permits
    NodeStarted,
    ElectionTimeout,
    HeartbeatTick,
    RequestVoteReceived,
    AppendEntriesReceived,
    VoteResponseReceived,
    AppendEntriesResponseReceived {
    default void fail(Throwable error) {}
}
