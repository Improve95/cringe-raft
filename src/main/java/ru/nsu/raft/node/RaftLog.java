package ru.nsu.raft.node;

import java.util.ArrayList;
import java.util.List;
import ru.nsu.raft.rpc.AppendEntriesRequest;

public class RaftLog {
    private final List<LogEntry> entries = new ArrayList<>();
    private long commitIndex;
    private LogSnapshot snapshot = new LogSnapshot(List.of(), 0);

    public long lastIndex() { return entries.size(); }
    public long lastTerm() { return termAt(lastIndex()); }
    public long commitIndex() { return commitIndex; }
    public long termAt(long index) { return index == 0 ? 0 : entries.get(Math.toIntExact(index - 1)).term(); }
    public List<LogEntry> after(long index) {
        return List.copyOf(entries.subList(Math.toIntExact(index), entries.size()));
    }
    public LogSnapshot snapshot() { return snapshot; }
    public LogEntry append(long term, String command) {
        var entry = new LogEntry(lastIndex() + 1, term, command);
        entries.add(entry);
        publish();
        return entry;
    }
    public boolean isUpToDate(long index, long term) {
        return term > lastTerm() || (term == lastTerm() && index >= lastIndex());
    }
    public boolean matches(long index, long term) {
        return index >= 0 && index <= lastIndex() && termAt(index) == term;
    }
    public boolean accept(AppendEntriesRequest request) {
        if (!matches(request.prevLogIndex(), request.prevLogTerm()) || !validEntries(request)) return false;
        for (var entry : request.entries()) {
            if (entry.index() <= lastIndex() && termAt(entry.index()) != entry.term()) {
                // Зафиксированный префикс нельзя удалять даже при некорректном RPC.
                if (entry.index() <= commitIndex) return false;
                entries.subList(Math.toIntExact(entry.index() - 1), entries.size()).clear();
            }
            if (entry.index() > lastIndex()) entries.add(entry);
        }
        long matched = request.prevLogIndex() + request.entries().size();
        // Нельзя фиксировать непроверенный хвост локального лога (§5.3).
        commitIndex = Math.max(commitIndex, Math.min(request.leaderCommit(), matched));
        publish();
        return true;
    }
    private boolean validEntries(AppendEntriesRequest request) {
        long index = request.prevLogIndex();
        long term = request.prevLogTerm();
        for (var entry : request.entries()) {
            if (entry.index() != ++index || entry.term() < term || entry.term() > request.term()) return false;
            term = entry.term();
        }
        return request.leaderCommit() >= 0;
    }
    public void commit(long index) {
        if (index > commitIndex) {
            commitIndex = Math.min(index, lastIndex());
            publish();
        }
    }
    private void publish() { snapshot = new LogSnapshot(entries, commitIndex); }
}
