package ru.nsu.raft.api;

import org.springframework.web.bind.annotation.*;
import ru.nsu.raft.node.*;

@RestController
public class StatusController {
    private final RaftNode node;
    public StatusController(RaftNode node) { this.node = node; }
    @GetMapping("/status")
    public NodeStatus status() { return node.snapshot().status(); }
    @GetMapping("/log")
    public LogSnapshot log() { return node.snapshot().log(); }
}
