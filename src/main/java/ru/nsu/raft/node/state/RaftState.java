package ru.nsu.raft.node.state;

import java.util.concurrent.CompletableFuture;
import ru.nsu.raft.api.AddEntryResponse;
import ru.nsu.raft.node.NodeRole;
import ru.nsu.raft.node.event.RaftEvent.AppendEntriesResponseReceived;
import ru.nsu.raft.rpc.*;

public interface RaftState {
    NodeRole role();
    void onEnter();
    void onExit();
    void onElectionTimeout();
    void onHeartbeatTick();
    void onVoteResponse(String from, RequestVoteResponse response);
    void onAppendEntriesResponse(AppendEntriesResponseReceived event);
    RequestVoteResponse onRequestVote(RequestVoteRequest request);
    AppendEntriesResponse onAppendEntries(AppendEntriesRequest request);
    void onAddEntry(String command, CompletableFuture<AddEntryResponse> reply);
}
