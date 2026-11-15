package ru.nsu.raft.api;

public record EntryRequest(String command) {
    public EntryRequest {
        if (command == null || command.isBlank()) {
            throw new IllegalArgumentException("Command must not be blank");
        }
    }
}
