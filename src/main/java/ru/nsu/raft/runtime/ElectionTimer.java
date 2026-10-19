package ru.nsu.raft.runtime;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Component;
import ru.nsu.raft.config.ClusterProperties;
import ru.nsu.raft.node.event.RaftEvent.ElectionTimeout;

@Component
public class ElectionTimer {
    private final EventQueue queue;
    private final ClusterProperties properties;
    private final AtomicLong epoch = new AtomicLong();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture<?> task;

    public ElectionTimer(EventQueue queue, ClusterProperties properties) {
        this.queue = queue;
        this.properties = properties;
    }
    public void reset() {
        cancel();
        long current = epoch.get();
        long min = properties.electionTimeoutMin().toMillis();
        long max = properties.electionTimeoutMax().toMillis();
        long delay = min == max ? min : ThreadLocalRandom.current().nextLong(min, max);
        task = scheduler.schedule(() -> queue.post(new ElectionTimeout(current)), delay, TimeUnit.MILLISECONDS);
    }
    public void cancel() {
        epoch.incrementAndGet();
        if (task != null) task.cancel(false);
    }
    public boolean isCurrent(long value) { return epoch.get() == value; }
    @PreDestroy
    public void close() { scheduler.shutdownNow(); }
}
