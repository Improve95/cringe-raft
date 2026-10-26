package ru.nsu.raft.api;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.nsu.raft.node.NodeStatus;
import ru.nsu.raft.node.RaftNode;

@RestController
@RequiredArgsConstructor
public class StatusController {
    private final RaftNode node;

    @GetMapping("/status")
    public NodeStatus status() {
        return node.snapshot().status();
    }
}
