package ru.nsu.raft.runtime;

import java.util.concurrent.LinkedBlockingQueue;
import org.springframework.stereotype.Component;
import ru.nsu.raft.node.event.RaftEvent;

@Component
public class EventQueue {
    private final LinkedBlockingQueue<RaftEvent> events = new LinkedBlockingQueue<>();
    public void post(RaftEvent event) { events.add(event); }
    public RaftEvent take() throws InterruptedException { return events.take(); }
}
