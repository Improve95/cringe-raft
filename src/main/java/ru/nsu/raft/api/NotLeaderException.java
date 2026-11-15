package ru.nsu.raft.api;

import lombok.Getter;
import lombok.experimental.Accessors;

@Getter
@Accessors(fluent = true)
public final class NotLeaderException extends RuntimeException {
    private final String leaderId;

    public NotLeaderException(String leaderId) {
        super("Submit the command to the current leader");
        this.leaderId = leaderId;
    }
}
