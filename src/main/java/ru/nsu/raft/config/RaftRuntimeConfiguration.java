package ru.nsu.raft.config;

import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.nsu.raft.node.RaftNode;
import ru.nsu.raft.runtime.ElectionTimer;
import ru.nsu.raft.runtime.EventLoop;
import ru.nsu.raft.runtime.EventQueue;
import ru.nsu.raft.runtime.HeartbeatScheduler;

import java.util.concurrent.ScheduledThreadPoolExecutor;

@Configuration(proxyBeanMethods = false)
public class RaftRuntimeConfiguration {

    @Bean
    public EventQueue eventQueue() {
        return new EventQueue();
    }

    @Bean(destroyMethod = "shutdownNow")
    public ScheduledThreadPoolExecutor raftScheduler() {
        var scheduler = new ScheduledThreadPoolExecutor(1, Thread.ofPlatform().name("raft-timer-", 0).factory());
        scheduler.setRemoveOnCancelPolicy(true);
        return scheduler;
    }

    @Bean(destroyMethod = "stop")
    public ElectionTimer electionTimer(
        EventQueue queue, ScheduledThreadPoolExecutor scheduler,
        ClusterProperties properties
    ) {
        return new ElectionTimer(queue, scheduler, properties);
    }

    @Bean
    public HeartbeatScheduler heartbeatScheduler(
        EventQueue queue, ScheduledThreadPoolExecutor scheduler,
        ClusterProperties properties
    ) {
        return new HeartbeatScheduler(queue, scheduler, properties);
    }

    @Bean
    public EventLoop eventLoop(EventQueue queue, RaftNode node) {
        return new EventLoop(queue, node);
    }

    @Bean
    public ApplicationListener<ApplicationReadyEvent> raftStartup(ElectionTimer electionTimer, EventLoop eventLoop) {
        return _ -> {
            electionTimer.start();
            eventLoop.start();
        };
    }
}
