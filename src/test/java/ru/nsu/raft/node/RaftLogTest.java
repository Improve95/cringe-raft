package ru.nsu.raft.node;

import org.junit.jupiter.api.Test;
import ru.nsu.raft.node.logger.LogEntry;
import ru.nsu.raft.node.logger.RaftLog;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RaftLogTest {
    @Test
    void rejectsMismatchingPrefixWithoutChangingLog() {
        var log = new RaftLog();
        log.append(1, "kept");
        assertThat(log.appendFrom(1, 2, List.of(new LogEntry(2, 2, "rejected")))).isFalse();
        assertThat(log.appendFrom(3, 1, List.of())).isFalse();
        assertThat(log.lastIndex()).isEqualTo(1);
        assertThat(log.entryAt(1).command()).isEqualTo("kept");
    }

    @Test
    void refusesToOverwriteCommittedEntriesAndDoesNotDecreaseCommitIndex() {
        var log = new RaftLog();
        log.append(1, "committed");
        log.commitThrough(1);
        assertThat(log.appendFrom(0, 0, List.of(new LogEntry(1, 2, "conflict")))).isFalse();
        log.commitThrough(0);
        assertThat(log.commitIndex()).isEqualTo(1);
        assertThat(log.entryAt(1).command()).isEqualTo("committed");
        assertThatThrownBy(() -> log.commitThrough(2)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void duplicateAndShorterAppendRequestsDoNotTruncateMatchingEntries() {
        var log = new RaftLog();
        List<LogEntry> entries = List.of(new LogEntry(1, 1, "one"), new LogEntry(2, 1, "two"));
        assertThat(log.appendFrom(0, 0, entries)).isTrue();
        assertThat(log.appendFrom(0, 0, entries)).isTrue();
        assertThat(log.appendFrom(0, 0, entries.subList(0, 1))).isTrue();
        assertThat(log.appendFrom(0, 0, List.of())).isTrue();
        assertThat(log.snapshot().entries()).containsExactlyElementsOf(entries);
    }

    @Test
    void candidateFreshnessComparesTermBeforeLength() {
        var log = new RaftLog();
        log.append(2, "one");
        log.append(2, "two");
        assertThat(log.isCandidateUpToDate(100, 1)).isFalse();
        assertThat(log.isCandidateUpToDate(1, 2)).isFalse();
        assertThat(log.isCandidateUpToDate(2, 2)).isTrue();
        assertThat(log.isCandidateUpToDate(1, 3)).isTrue();
    }
}
