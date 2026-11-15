package ru.nsu.raft.node.state;

import ru.nsu.raft.node.NodeRole;
import ru.nsu.raft.node.event.AppendEntriesResponseReceived;
import ru.nsu.raft.node.event.ClientEntryReceived;
import ru.nsu.raft.rpc.AppendEntriesRequest;
import ru.nsu.raft.rpc.AppendEntriesResponse;
import ru.nsu.raft.rpc.RequestVoteRequest;
import ru.nsu.raft.rpc.RequestVoteResponse;

public interface RaftState {
    NodeRole role();

    void onEnter();

    void onExit();

    void onElectionTimeout();

    void onHeartbeatTick();

    void onVoteResponse(String from, RequestVoteResponse response);

    void onAppendEntriesResponse(AppendEntriesResponseReceived event);

    void onClientEntry(ClientEntryReceived event);

    RequestVoteResponse onRequestVote(RequestVoteRequest request);

    AppendEntriesResponse onAppendEntries(AppendEntriesRequest request);
}
