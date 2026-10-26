package ru.nsu.raft.runtime;

import ru.nsu.raft.config.ClusterProperties;
import ru.nsu.raft.node.event.HeartbeatTick;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public final class HeartbeatScheduler {
    private final EventQueue queue;
    private final ScheduledExecutorService scheduler;
    private final long intervalMillis;
    private ScheduledFuture<?> scheduled;

    public HeartbeatScheduler(EventQueue queue, ScheduledExecutorService scheduler, ClusterProperties properties) {
        this.queue = queue;
        this.scheduler = scheduler;
        intervalMillis = properties.heartbeatInterval().toMillis();
    }

    public void start() {
        stop();
        scheduled = scheduler.scheduleAtFixedRate(
            () -> queue.post(new HeartbeatTick()),
            intervalMillis,
            intervalMillis,
            TimeUnit.MILLISECONDS);
    }

    public void stop() {
        if (scheduled != null) {
            scheduled.cancel(false);
            scheduled = null;
        }
    }
}
