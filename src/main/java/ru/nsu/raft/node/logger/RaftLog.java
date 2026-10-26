package ru.nsu.raft.node.logger;

import java.util.ArrayList;
import java.util.List;

public final class RaftLog {
    private final List<LogEntry> entries = new ArrayList<>();
    private long commitIndex;
    private LogSnapshot snapshot = new LogSnapshot(List.of(), 0);

    public long lastIndex() {
        return entries.size();
    }

    public long lastTerm() {
        return termAt(lastIndex());
    }

    public long commitIndex() {
        return commitIndex;
    }

    public long termAt(long index) {
        return index == 0 ? 0 : entryAt(index).term();
    }

    public LogEntry entryAt(long index) {
        return entries.get(Math.toIntExact(index - 1));
    }

    public boolean isCandidateUpToDate(long index, long term) {
        return term > lastTerm() || (term == lastTerm() && index >= lastIndex());
    }

    public LogEntry append(long term, String command) {
        var entry = new LogEntry(lastIndex() + 1, term, command);
        entries.add(entry);
        snapshot = null;
        return entry;
    }

    public List<LogEntry> entriesFrom(long index) {
        return List.copyOf(entries.subList(Math.toIntExact(index - 1), entries.size()));
    }

    public boolean appendFrom(long previousIndex, long previousTerm, List<LogEntry> incoming) {
        if (!matches(previousIndex, previousTerm)) {
            return false;
        }
        for (int offset = 0; offset < incoming.size(); offset++) {
            LogEntry entry = incoming.get(offset);
            if (entry.index() > lastIndex() || termAt(entry.index()) != entry.term()) {
                return replaceSuffix(entry.index(), incoming.subList(offset, incoming.size()));
            }
        }
        return true;
    }

    public void commitThrough(long index) {
        if (index > lastIndex()) {
            throw new IllegalArgumentException("Cannot commit beyond the log");
        }
        if (index > commitIndex) {
            commitIndex = index;
            snapshot = null;
        }
    }

    public LogSnapshot snapshot() {
        if (snapshot == null) {
            snapshot = new LogSnapshot(entries, commitIndex);
        }
        return snapshot;
    }

    private boolean matches(long index, long term) {
        return index >= 0 && index <= lastIndex() && termAt(index) == term;
    }

    private boolean replaceSuffix(long index, List<LogEntry> suffix) {
        // Зафиксированный префикс неизменяем: Raft §5.4, Leader Completeness.
        if (index <= commitIndex) {
            return false;
        }
        entries.subList(Math.toIntExact(index - 1), entries.size()).clear();
        entries.addAll(suffix);
        snapshot = null;
        return true;
    }
}
