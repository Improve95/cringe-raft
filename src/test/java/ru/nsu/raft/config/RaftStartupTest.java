package ru.nsu.raft.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import ru.nsu.raft.node.RaftNode;
import ru.nsu.raft.node.event.NodeStarted;
import ru.nsu.raft.node.event.RequestVoteReceived;
import ru.nsu.raft.rpc.RequestVoteRequest;
import ru.nsu.raft.runtime.ElectionTimer;
import ru.nsu.raft.runtime.EventLoop;
import ru.nsu.raft.runtime.EventQueue;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

class RaftStartupTest {
    @Test
    void startsOnlyWhenApplicationIsReadyAndInitializesBeforeQueuedRequests() throws Exception {
        var node = mock(RaftNode.class);
        var processed = new CompletableFuture<Void>();
        try (var context = new AnnotationConfigApplicationContext()) {
            context.registerBean(RaftNode.class, () -> node);
            context.registerBean(ClusterProperties.class, () -> new ClusterProperties(
                "a", Map.of("a", URI.create("http://localhost:8081")),
                Duration.ofSeconds(30), Duration.ofSeconds(30), Duration.ofMillis(25),
                Duration.ofMillis(75), Duration.ofSeconds(1)));
            context.register(RaftRuntimeConfiguration.class);
            context.refresh();

            var timer = context.getBean(ElectionTimer.class);
            var loop = context.getBean(EventLoop.class);
            var scheduler = context.getBean(ScheduledThreadPoolExecutor.class);
            var request = new RequestVoteReceived(new RequestVoteRequest(1, "b"), new CompletableFuture<>());
            context.getBean(EventQueue.class).post(request);

            assertThat(loop.isRunning()).isFalse();
            assertThat(timer.epoch()).isZero();
            assertThat(scheduler.getQueue()).isEmpty();
            verifyNoInteractions(node);

            doAnswer(call -> {
                timer.reset();
                return null;
            }).when(node).handle(isA(NodeStarted.class));
            doAnswer(call -> {
                processed.complete(null);
                return null;
            }).when(node).handle(request);

            context.publishEvent(new ApplicationReadyEvent(
                new SpringApplication(RaftRuntimeConfiguration.class), new String[0], context, Duration.ofSeconds(10)));
            processed.get(2, TimeUnit.SECONDS);

            assertThat(loop.isRunning()).isTrue();
            assertThat(timer.isCurrent(timer.epoch())).isTrue();
            assertThat(scheduler.getTaskCount()).isPositive();
            var order = inOrder(node);
            order.verify(node).handle(new NodeStarted());
            order.verify(node).handle(request);
        }
        verify(node).stop();
    }
}
