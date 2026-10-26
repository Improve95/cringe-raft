package ru.nsu.raft.api;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import ru.nsu.raft.config.ClusterProperties;
import ru.nsu.raft.node.RaftNode;
import ru.nsu.raft.node.event.ClientEntryReceived;
import ru.nsu.raft.node.logger.LogSnapshot;
import ru.nsu.raft.runtime.EventQueue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@RestController
public class LogController {
    private final RaftNode node;
    private final EventQueue queue;
    private final long timeoutMillis;

    public LogController(RaftNode node, EventQueue queue, ClusterProperties properties) {
        this.node = node;
        this.queue = queue;
        timeoutMillis = properties.rpcTimeout().toMillis();
    }

    @GetMapping("/log")
    public LogSnapshot log() {
        return node.snapshot().log();
    }

    @PostMapping("/entries")
    public CompletableFuture<EntryResponse> addEntry(@RequestBody EntryRequest request) {
        var reply = new CompletableFuture<EntryResponse>().orTimeout(timeoutMillis, TimeUnit.MILLISECONDS);
        queue.post(new ClientEntryReceived(request, reply));
        return reply;
    }
}
