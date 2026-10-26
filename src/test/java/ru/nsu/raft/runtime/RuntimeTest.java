package ru.nsu.raft.runtime;

import org.junit.jupiter.api.Test;
import ru.nsu.raft.config.ClusterProperties;
import ru.nsu.raft.node.RaftNode;
import ru.nsu.raft.node.event.ElectionTimeout;
import ru.nsu.raft.node.event.NodeStarted;
import ru.nsu.raft.node.event.RequestVoteReceived;
import ru.nsu.raft.rpc.RequestVoteRequest;
import ru.nsu.raft.rpc.RequestVoteResponse;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

class RuntimeTest {
    @Test
    void eventLoopSurvivesAHandlerExceptionAndProcessesTheNextEvent() throws Exception {
        RaftNode node = mock(RaftNode.class);
        var queue = new EventQueue();
        var loop = new EventLoop(queue, node);
        doThrow(new IllegalStateException("broken event")).when(node).handle(isA(RequestVoteReceived.class));
        var processed = new CompletableFuture<Void>();
        doAnswer(call -> {
            processed.complete(null);
            return null;
        }).when(node).handle(isA(ElectionTimeout.class));
        try {
            loop.start();
            var failed = new CompletableFuture<RequestVoteResponse>();
            queue.post(new RequestVoteReceived(new RequestVoteRequest(1, "b", 0, 0), failed));
            queue.post(new ElectionTimeout(2));
            processed.get(2, TimeUnit.SECONDS);
            assertThat(failed).isCompletedExceptionally();
            assertThat(loop.isRunning()).isTrue();
            verify(node).handle(isA(NodeStarted.class));
        } finally {
            var stopped = new CompletableFuture<Void>();
            loop.stop(() -> stopped.complete(null));
            stopped.get(2, TimeUnit.SECONDS);
        }
        verify(node, timeout(2000)).stop();
    }

    @Test
    void timerPostsEventsAndResetAndCancellationInvalidateTheirEpochs() throws Exception {
        var queue = new EventQueue();
        var properties = new ClusterProperties(
            "a", List.of(), Duration.ofMillis(20),
            Duration.ofMillis(20), Duration.ofMillis(5), Duration.ofSeconds(1));
        try (var scheduler = Executors.newSingleThreadScheduledExecutor()) {
            var timer = new ElectionTimer(queue, scheduler, properties);
            timer.reset();
            var event = CompletableFuture.supplyAsync(() -> {
                try {
                    return (ElectionTimeout) queue.take();
                } catch (InterruptedException error) {
                    throw new IllegalStateException(error);
                }
            }).get(2, TimeUnit.SECONDS);
            assertThat(timer.isCurrent(event.epoch())).isTrue();
            timer.reset();
            assertThat(timer.isCurrent(event.epoch())).isFalse();
            long resetEpoch = timer.epoch();
            timer.cancel();
            assertThat(timer.isCurrent(resetEpoch)).isFalse();
        }
    }
}
