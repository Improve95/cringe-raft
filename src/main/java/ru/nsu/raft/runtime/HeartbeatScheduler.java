package ru.nsu.raft.runtime;

import java.util.concurrent.*;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Component;
import ru.nsu.raft.config.ClusterProperties;
import ru.nsu.raft.node.event.RaftEvent.HeartbeatTick;

@Component
public class HeartbeatScheduler {
    private final EventQueue queue;
    private final ClusterProperties properties;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture<?> task;

    public HeartbeatScheduler(EventQueue queue, ClusterProperties properties) {
        this.queue = queue;
        this.properties = properties;
    }
    public void start() {
        stop();
        long interval = properties.heartbeatInterval().toMillis();
        task = scheduler.scheduleAtFixedRate(() -> queue.post(new HeartbeatTick()),
                interval, interval, TimeUnit.MILLISECONDS);
    }
    public void stop() { if (task != null) task.cancel(false); }
    @PreDestroy
    public void close() { scheduler.shutdownNow(); }
}
