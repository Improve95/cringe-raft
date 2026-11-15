package ru.nsu.raft.runtime;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import ru.nsu.raft.config.ClusterProperties;
import ru.nsu.raft.node.event.ElectionTimeout;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

class ElectionTimerTest {
    private final EventQueue queue = mock(EventQueue.class);
    private final ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    private final ScheduledFuture<?> periodicCheck = mock(ScheduledFuture.class);
    private final AtomicLong now = new AtomicLong();
    private ElectionTimer timer;
    private Runnable check;

    @BeforeEach
    void setUp() {
        var properties = new ClusterProperties(
            "a", Map.of("a", URI.create("http://localhost:8081")),
            Duration.ofMillis(100), Duration.ofMillis(100), Duration.ofMillis(10),
            Duration.ofMillis(25), Duration.ofSeconds(1));
        doReturn(periodicCheck).when(scheduler)
            .scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), eq(TimeUnit.NANOSECONDS));
        timer = new ElectionTimer(queue, scheduler, properties, now::get);
        timer.start();
        var callback = ArgumentCaptor.forClass(Runnable.class);
        verify(scheduler).scheduleWithFixedDelay(callback.capture(), eq(10_000_000L), eq(10_000_000L), eq(TimeUnit.NANOSECONDS));
        check = callback.getValue();
    }

    @Test
    void firstResetStartsAFullTimeoutRegardlessOfApplicationStartupTime() {
        now.set(TimeUnit.SECONDS.toNanos(10));
        check.run();
        assertThat(timer.isCurrent(timer.epoch())).isFalse();
        verifyNoInteractions(queue);

        timer.reset();
        check.run();
        now.addAndGet(TimeUnit.MILLISECONDS.toNanos(99));
        check.run();
        verifyNoInteractions(queue);

        now.addAndGet(TimeUnit.MILLISECONDS.toNanos(1));
        check.run();
        verify(queue).post(new ElectionTimeout(timer.epoch()));
    }

    @Test
    void heartbeatMovesTimeoutWithoutRescheduling() {
        timer.reset();
        now.set(99_000_000L);
        check.run();
        verifyNoInteractions(queue);

        timer.reset();
        now.set(100_000_000L);
        check.run();
        verifyNoInteractions(queue);
        now.set(199_000_000L);
        check.run();
        verify(queue).post(new ElectionTimeout(timer.epoch()));
        verifyNoMoreInteractions(scheduler);
        verifyNoInteractions(periodicCheck);
    }

    @Test
    void postsOncePerEpochAndInvalidatesQueuedTimeoutOnReset() {
        timer.reset();
        long oldEpoch = timer.epoch();
        now.set(100_000_000L);
        check.run();
        check.run();
        verify(queue).post(new ElectionTimeout(oldEpoch));
        verifyNoMoreInteractions(queue);

        timer.reset();
        assertThat(timer.isCurrent(oldEpoch)).isFalse();
        now.set(200_000_000L);
        check.run();
        verify(queue).post(new ElectionTimeout(timer.epoch()));
        assertThat(timer.isCurrent(timer.epoch())).isTrue();
    }

    @Test
    void cancellationDisablesChecksUntilResetWithoutStoppingScheduler() {
        check.run();
        timer.reset();
        long oldEpoch = timer.epoch();
        timer.cancel();
        now.set(100_000_000L);
        check.run();
        assertThat(timer.isCurrent(oldEpoch)).isFalse();
        assertThat(timer.isCurrent(timer.epoch())).isFalse();
        verifyNoInteractions(queue, periodicCheck);

        timer.reset();
        now.set(200_000_000L);
        check.run();
        verify(queue).post(new ElectionTimeout(timer.epoch()));
        verifyNoMoreInteractions(scheduler);
    }

    @Test
    void startsOnePeriodicTaskAndStopsItWithoutInterruption() {
        timer.start();
        verifyNoMoreInteractions(scheduler);
        timer.reset();
        timer.stop();
        now.set(100_000_000L);
        check.run();
        verify(periodicCheck).cancel(false);
        verifyNoInteractions(queue);
    }
}
