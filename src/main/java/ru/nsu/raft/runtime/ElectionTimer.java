package ru.nsu.raft.runtime;

import ru.nsu.raft.config.ClusterProperties;
import ru.nsu.raft.node.event.ElectionTimeout;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public final class ElectionTimer {
    private final EventQueue queue;
    private final ScheduledExecutorService scheduler;
    private final long minimumMillis;
    private final long maximumMillis;
    private final AtomicLong epoch = new AtomicLong();
    private ScheduledFuture<?> scheduled;

    public ElectionTimer(EventQueue queue, ScheduledExecutorService scheduler, ClusterProperties properties) {
        this.queue = queue;
        this.scheduler = scheduler;
        minimumMillis = properties.electionTimeoutMin().toMillis();
        maximumMillis = properties.electionTimeoutMax().toMillis();
    }

    public void reset() {
        cancel();
        long scheduledEpoch = epoch.get();
        long delay = minimumMillis == maximumMillis ? minimumMillis
            : ThreadLocalRandom.current().nextLong(minimumMillis, maximumMillis);
        scheduled = scheduler.schedule(
            () -> queue.post(new ElectionTimeout(scheduledEpoch)),
            delay, TimeUnit.MILLISECONDS);
    }

    public void cancel() {
        epoch.incrementAndGet();
        if (scheduled != null) {
            scheduled.cancel(false);
            scheduled = null;
        }
    }

    public boolean isCurrent(long eventEpoch) {
        return eventEpoch == epoch.get();
    }

    public long epoch() {
        return epoch.get();
    }
}
