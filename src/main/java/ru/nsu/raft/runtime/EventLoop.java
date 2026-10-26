package ru.nsu.raft.runtime;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;
import ru.nsu.raft.node.RaftNode;
import ru.nsu.raft.node.event.NodeStarted;
import ru.nsu.raft.node.event.RaftEvent;

import java.util.concurrent.CompletableFuture;

@Slf4j
@RequiredArgsConstructor
public final class EventLoop implements SmartLifecycle {
    private final EventQueue queue;
    private final RaftNode node;
    @Getter
    private volatile boolean running;
    private CompletableFuture<Void> termination = CompletableFuture.completedFuture(null);
    private Thread thread;

    @Override
    public void start() {
        if (running) {
            return;
        }
        queue.post(new NodeStarted());
        termination = new CompletableFuture<>();
        running = true;
        thread = Thread.ofPlatform().name("raft-event-loop").start(this::run);
    }

    @Override
    public void stop() {
        running = false;
        if (thread != null) {
            thread.interrupt();
        }
    }

    @Override
    public void stop(Runnable callback) {
        stop();
        // Spring должен дождаться выхода EventLoop, прежде чем закрывать транспорт и таймеры.
        termination.thenRun(callback);
    }

    private void run() {
        try {
            while (running) {
                dispatch(queue.take());
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        } finally {
            running = false;
            try {
                node.stop();
            } finally {
                termination.complete(null);
            }
        }
    }

    private void dispatch(RaftEvent event) {
        try {
            node.handle(event);
        } catch (Exception error) {
            event.fail(error);
            log.error("Failed to handle {}", event.getClass().getSimpleName(), error);
        }
    }
}
