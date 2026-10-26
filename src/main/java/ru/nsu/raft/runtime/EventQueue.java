package ru.nsu.raft.runtime;

import ru.nsu.raft.node.event.RaftEvent;

import java.util.concurrent.LinkedBlockingQueue;

public final class EventQueue {
    private final LinkedBlockingQueue<RaftEvent> events = new LinkedBlockingQueue<>();

    public void post(RaftEvent event) {
        events.add(event);
    }

    public RaftEvent take() throws InterruptedException {
        return events.take();
    }
}
