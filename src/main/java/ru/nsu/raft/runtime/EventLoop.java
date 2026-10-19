package ru.nsu.raft.runtime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;
import ru.nsu.raft.node.RaftNode;
import ru.nsu.raft.node.event.RaftEvent;

@Component
public class EventLoop implements SmartLifecycle {
    private static final Logger log = LoggerFactory.getLogger(EventLoop.class);
    private final EventQueue queue;
    private final RaftNode node;
    private volatile boolean running;
    private Thread thread;

    public EventLoop(EventQueue queue, RaftNode node) { this.queue = queue; this.node = node; }
    @Override
    public void start() {
        queue.post(new RaftEvent.NodeStarted());
        running = true;
        thread = Thread.ofPlatform().name("raft-event-loop").start(this::run);
    }
    private void run() {
        try {
            while (running) {
                var event = queue.take();
                try { node.handle(event); }
                catch (Exception exception) { log.error("Failed to handle {}", event, exception); }
            }
        } catch (InterruptedException exception) { Thread.currentThread().interrupt(); }
        finally { running = false; }
    }
    @Override public void stop() {
        running = false;
        if (thread == null) return;
        thread.interrupt();
        try { thread.join(); }
        catch (InterruptedException exception) { Thread.currentThread().interrupt(); }
    }
    @Override public boolean isRunning() { return running; }
}
