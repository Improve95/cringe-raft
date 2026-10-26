package ru.nsu.raft.config;

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
    EventQueue eventQueue() {
        return new EventQueue();
    }

    @Bean(destroyMethod = "shutdownNow")
    ScheduledThreadPoolExecutor raftScheduler() {
        var scheduler = new ScheduledThreadPoolExecutor(1, Thread.ofPlatform().name("raft-timer-", 0).factory());
        scheduler.setRemoveOnCancelPolicy(true);
        return scheduler;
    }

    @Bean
    ElectionTimer electionTimer(
        EventQueue queue, ScheduledThreadPoolExecutor scheduler,
        ClusterProperties properties
    ) {
        return new ElectionTimer(queue, scheduler, properties);
    }

    @Bean
    HeartbeatScheduler heartbeatScheduler(
        EventQueue queue, ScheduledThreadPoolExecutor scheduler,
        ClusterProperties properties
    ) {
        return new HeartbeatScheduler(queue, scheduler, properties);
    }

    @Bean
    EventLoop eventLoop(EventQueue queue, RaftNode node) {
        return new EventLoop(queue, node);
    }
}
