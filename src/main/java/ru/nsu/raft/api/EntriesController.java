package ru.nsu.raft.api;

import java.util.concurrent.*;
import org.springframework.web.bind.annotation.*;
import ru.nsu.raft.config.ClusterProperties;
import ru.nsu.raft.node.event.RaftEvent.AddEntryReceived;
import ru.nsu.raft.runtime.EventQueue;

@RestController
public class EntriesController {
    private final EventQueue queue;
    private final long timeoutMillis;
    public EntriesController(EventQueue queue, ClusterProperties properties) {
        this.queue = queue;
        timeoutMillis = properties.rpcTimeout().toMillis();
    }
    @PostMapping("/entries")
    public CompletableFuture<AddEntryResponse> add(@RequestBody AddEntryRequest request) {
        var reply = new CompletableFuture<AddEntryResponse>();
        queue.post(new AddEntryReceived(request.command(), reply));
        return reply.orTimeout(timeoutMillis, TimeUnit.MILLISECONDS);
    }
    public record AddEntryRequest(String command) {
        public AddEntryRequest {
            if (command == null || command.isBlank()) throw new IllegalArgumentException("command must not be blank");
        }
    }
}
