package ru.nsu.raft.runtime;

import lombok.extern.slf4j.Slf4j;
import ru.nsu.raft.config.ClusterProperties;
import ru.nsu.raft.node.event.ElectionTimeout;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

@Slf4j
public final class ElectionTimer {
    private final EventQueue queue;
    private final ScheduledExecutorService scheduler;
    private final long minimumNanos;
    private final long maximumNanos;
    private final long checkIntervalNanos;
    private final LongSupplier nanoTime;
    private volatile TimerState state = new TimerState(0, 0, 0, false);
    private long lastPostedEpoch;
    private ScheduledFuture<?> periodicCheck;

    public ElectionTimer(EventQueue queue, ScheduledExecutorService scheduler, ClusterProperties properties) {
        this(queue, scheduler, properties, System::nanoTime);
    }

    ElectionTimer(EventQueue queue, ScheduledExecutorService scheduler, ClusterProperties properties, LongSupplier nanoTime) {
        this.queue = queue;
        this.scheduler = scheduler;
        this.nanoTime = nanoTime;
        minimumNanos = properties.electionTimeoutMin().toNanos();
        maximumNanos = properties.electionTimeoutMax().toNanos();
        checkIntervalNanos = properties.electionCheckInterval().toNanos();
    }

    public void start() {
        if (periodicCheck == null) {
            log.info("Election timer started");
            periodicCheck = scheduler.scheduleWithFixedDelay(
                this::checkTimeout,
                checkIntervalNanos,
                checkIntervalNanos,
                TimeUnit.NANOSECONDS);
            // Первый reset выполняет NodeStarted в EventLoop: время создания bean не входит в timeout.
        }
    }

    public void stop() {
        cancel();
        if (periodicCheck != null) {
            periodicCheck.cancel(false);
            periodicCheck = null;
        }
    }

    public void reset() {
        long timeout = minimumNanos == maximumNanos
            ? minimumNanos
            : ThreadLocalRandom.current().nextLong(minimumNanos, maximumNanos);
        state = new TimerState(state.epoch() + 1, nanoTime.getAsLong(), timeout, true);
    }

    public void cancel() {
        state = new TimerState(state.epoch() + 1, 0, 0, false);
    }

    public boolean isCurrent(long eventEpoch) {
        TimerState current = state;
        return current.enabled() && eventEpoch == current.epoch();
    }

    public long epoch() {
        return state.epoch();
    }

    private void checkTimeout() {
        TimerState current = state;
        if (!current.enabled() || current.epoch() == lastPostedEpoch) {
            return;
        }
        if (nanoTime.getAsLong() - current.lastResetNanos() >= current.timeoutNanos()) {
            lastPostedEpoch = current.epoch();
            queue.post(new ElectionTimeout(current.epoch()));
        }
    }

    private record TimerState(long epoch, long lastResetNanos, long timeoutNanos, boolean enabled) {}
}
