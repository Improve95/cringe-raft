package ru.nsu.raft.runtime;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import ru.nsu.raft.config.ClusterProperties;
import ru.nsu.raft.node.RaftNode;
import ru.nsu.raft.node.event.RaftEvent.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class RuntimeTests {
    @Test void eventLoopContinuesAfterHandlerThrows() throws Exception {
        var queue = new EventQueue();
        var node = mock(RaftNode.class);
        var handled = new CompletableFuture<String>();
        doThrow(new IllegalStateException("test failure")).when(node).handle(new NodeStarted());
        doAnswer(invocation -> {
            handled.complete(Thread.currentThread().getName());
            return null;
        }).when(node).handle(new HeartbeatTick());
        var loop = new EventLoop(queue, node);
        try {
            loop.start();
            queue.post(new HeartbeatTick());
            assertEquals("raft-event-loop", handled.get(2, TimeUnit.SECONDS));
            assertTrue(loop.isRunning());
        } finally { loop.stop(); }
        assertFalse(loop.isRunning());
    }
    @Test void resettingAndCancellingInvalidateAlreadyQueuedTimeouts() throws Exception {
        var queue = new EventQueue();
        var properties = new ClusterProperties("n1", List.of(), Duration.ofMillis(20),
                Duration.ofMillis(20), Duration.ofMillis(5), Duration.ofSeconds(1));
        var timer = new ElectionTimer(queue, properties);
        try {
            timer.reset();
            try (var executor = Executors.newSingleThreadExecutor()) {
                var fired = (ElectionTimeout) executor.submit(queue::take).get(2, TimeUnit.SECONDS);
                assertTrue(timer.isCurrent(fired.epoch()));
                timer.reset();
                assertFalse(timer.isCurrent(fired.epoch()));
                var next = (ElectionTimeout) executor.submit(queue::take).get(2, TimeUnit.SECONDS);
                assertTrue(timer.isCurrent(next.epoch()));
                timer.cancel();
                assertFalse(timer.isCurrent(next.epoch()));
            }
        } finally { timer.close(); }
    }
}
