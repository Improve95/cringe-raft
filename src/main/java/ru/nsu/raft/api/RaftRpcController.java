package ru.nsu.raft.api;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.springframework.web.bind.annotation.*;
import ru.nsu.raft.config.ClusterProperties;
import ru.nsu.raft.node.event.RaftEvent.*;
import ru.nsu.raft.rpc.*;
import ru.nsu.raft.runtime.EventQueue;

@RestController
@RequestMapping("/raft")
public class RaftRpcController {
    private final EventQueue queue;
    private final long timeoutMillis;
    public RaftRpcController(EventQueue queue, ClusterProperties properties) {
        this.queue = queue;
        timeoutMillis = properties.rpcTimeout().toMillis();
    }
    @PostMapping("/request-vote")
    public CompletableFuture<RequestVoteResponse> requestVote(@RequestBody RequestVoteRequest request) {
        var reply = new CompletableFuture<RequestVoteResponse>();
        queue.post(new RequestVoteReceived(request, reply));
        return reply.orTimeout(timeoutMillis, TimeUnit.MILLISECONDS);
    }
    @PostMapping("/append-entries")
    public CompletableFuture<AppendEntriesResponse> appendEntries(@RequestBody AppendEntriesRequest request) {
        var reply = new CompletableFuture<AppendEntriesResponse>();
        queue.post(new AppendEntriesReceived(request, reply));
        return reply.orTimeout(timeoutMillis, TimeUnit.MILLISECONDS);
    }
}
