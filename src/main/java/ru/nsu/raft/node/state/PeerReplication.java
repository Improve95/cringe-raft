package ru.nsu.raft.node.state;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.experimental.Accessors;
import ru.nsu.raft.node.event.AppendEntriesResponseReceived;

@Accessors(fluent = true)
final class PeerReplication {
    @Getter(AccessLevel.PACKAGE)
    private long nextIndex;
    @Getter(AccessLevel.PACKAGE)
    private long matchIndex;
    private long sequence;
    private long lastHandled;

    PeerReplication(long nextIndex) {
        this.nextIndex = nextIndex;
    }

    long nextRequestId() {
        return ++sequence;
    }

    boolean acknowledge(AppendEntriesResponseReceived event) {
        // Ответ относится к исходному запросу: запоздалый отказ не отменяет новый успех.
        if (event.requestId() <= lastHandled || event.requestId() > sequence) {
            return false;
        }
        lastHandled = event.requestId();
        if (event.response().success()) {
            matchIndex = Math.max(matchIndex, event.request().lastIndex());
            nextIndex = Math.max(nextIndex, matchIndex + 1);
        } else {
            nextIndex = Math.max(1, Math.min(nextIndex - 1, event.request().prevLogIndex()));
            matchIndex = Math.min(matchIndex, nextIndex - 1);
        }
        return true;
    }
}
