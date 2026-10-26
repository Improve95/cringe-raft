package ru.nsu.raft.api;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.nsu.raft.node.NodeStatus;
import ru.nsu.raft.node.RaftNode;

@RestController
public class StatusController {
    private final RaftNode node;

    public StatusController(RaftNode node) {
        this.node = node;
    }

    @GetMapping("/status")
    public NodeStatus status() {
        return node.snapshot().status();
    }
}
