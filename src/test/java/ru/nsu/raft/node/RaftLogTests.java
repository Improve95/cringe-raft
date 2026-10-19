package ru.nsu.raft.node;

import java.util.List;
import org.junit.jupiter.api.Test;
import ru.nsu.raft.rpc.AppendEntriesRequest;
import static org.junit.jupiter.api.Assertions.*;

class RaftLogTests {
    private final RaftLog log = new RaftLog();
    private AppendEntriesRequest request(long prev, long term, long commit, LogEntry... entries) {
        return new AppendEntriesRequest(5, "leader", prev, term, List.of(entries), commit);
    }
    @Test void replacesOnlyConflictingSuffix() {
        log.append(1, "a");
        log.append(2, "old");
        log.append(2, "tail");
        assertTrue(log.accept(request(1, 1, 2, new LogEntry(2, 3, "new"))));
        assertEquals(List.of(new LogEntry(1, 1, "a"), new LogEntry(2, 3, "new")), log.snapshot().entries());
        assertEquals(2, log.commitIndex());
    }
    @Test void heartbeatDoesNotTruncateSuffixOrCommitUnverifiedEntries() {
        log.append(1, "a");
        log.append(2, "unverified");
        assertTrue(log.accept(request(1, 1, 2)));
        assertEquals(2, log.lastIndex());
        assertEquals(1, log.commitIndex());
    }
    @Test void refusesWrongPrefixAndCommittedConflict() {
        log.append(1, "a");
        log.commit(1);
        assertFalse(log.accept(request(1, 2, 0)));
        assertFalse(log.accept(request(0, 0, 1, new LogEntry(1, 3, "conflict"))));
        assertEquals("a", log.snapshot().entries().getFirst().command());
    }
    @Test void rejectsInvalidBatchBeforeMutation() {
        log.append(1, "a");
        assertFalse(log.accept(request(0, 0, 0, new LogEntry(1, 2, "b"), new LogEntry(3, 2, "gap"))));
        assertEquals(1, log.lastTerm());
    }
    @Test void comparesLastTermBeforeLastIndex() {
        log.append(2, "a");
        assertFalse(log.isUpToDate(100, 1));
        assertFalse(log.isUpToDate(0, 2));
        assertTrue(log.isUpToDate(1, 2));
        assertTrue(log.isUpToDate(0, 3));
    }
    @Test void snapshotIsImmutableAndDetached() {
        log.append(1, "a");
        var snapshot = log.snapshot();
        log.append(1, "b");
        assertEquals(1, snapshot.entries().size());
        assertThrows(UnsupportedOperationException.class, () -> snapshot.entries().clear());
    }
}
