package ru.nsu.raft.api;

import io.swagger.v3.oas.annotations.media.Schema;

public record EntryRequest(@Schema(example = "hello") String command) {
    public EntryRequest {
        if (command == null || command.isBlank()) {
            throw new IllegalArgumentException("Command must not be blank");
        }
    }
}
