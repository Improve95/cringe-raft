package ru.nsu.raft.node.event;

public record ElectionTimeout(long epoch) implements RaftEvent {
}
