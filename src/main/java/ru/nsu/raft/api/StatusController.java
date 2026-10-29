package ru.nsu.raft.api;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.nsu.raft.node.NodeStatus;
import ru.nsu.raft.node.RaftNode;

@RestController
@RequiredArgsConstructor
@Tag(name = "Состояние узла", description = "Состояние локального узла Raft")
public class StatusController {
    private final RaftNode node;

    @GetMapping("/status")
    @Operation(summary = "Получить роль, терм и известного лидера")
    public NodeStatus status() {
        return node.snapshot().status();
    }
}
