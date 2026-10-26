package ru.nsu.raft.node;

import org.junit.jupiter.api.Test;
import ru.nsu.raft.api.NotLeaderException;
import ru.nsu.raft.node.event.HeartbeatTick;
import ru.nsu.raft.node.logger.LogEntry;
import ru.nsu.raft.rpc.AppendEntriesRequest;
import ru.nsu.raft.support.NodeFixture;

import java.util.List;
import java.util.concurrent.CompletionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;

class ReplicationTest {
    @Test
    void clientReplyWaitsForMajorityOfTheWholeCluster() {
        var fixture = new NodeFixture("b", "c", "d", "e");
        fixture.timeout();
        fixture.vote("b", 1, true);
        fixture.vote("c", 1, true);
        var reply = fixture.submit("command");
        assertThat(reply).isNotDone();
        fixture.acknowledge(fixture.lastAppend("b"), 1, true);
        assertThat(reply).isNotDone();
        assertThat(fixture.log.commitIndex()).isZero();
        fixture.acknowledge(fixture.lastAppend("c"), 1, true);
        assertThat(reply.join().index()).isEqualTo(2);
        assertThat(fixture.log.commitIndex()).isEqualTo(2);
        assertThat(fixture.lastAppend("d").request().leaderCommit()).isEqualTo(2);
    }

    @Test
    void isolatedLeaderCannotCommitAndFailsPendingCommandsWhenItStepsDown() {
        var fixture = electedLeader();
        var reply = fixture.submit("uncommitted");
        fixture.node.handle(new HeartbeatTick());
        assertThat(reply).isNotDone();
        assertThat(fixture.log.commitIndex()).isZero();
        fixture.vote("b", 2, false);
        assertThatThrownBy(reply::join).isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(NotLeaderException.class);
        verify(fixture.heartbeat).stop();
    }

    @Test
    void sameTermRejectionsBacktrackUntilFollowerMatchesAndReplaceItsConflictingTail() {
        var leader = electedLeader();
        leader.submit("correct");
        var follower = new NodeFixture("b", "c");
        follower.log.append(0, "conflicting");
        follower.log.append(0, "extra");
        synchronize(leader, follower);
        assertThat(follower.log.snapshot()).isEqualTo(leader.log.snapshot());
        assertThat(follower.log.entryAt(2).command()).isEqualTo("correct");
    }

    @Test
    void restartedEmptyFollowerCanRecoverEvenAfterItPreviouslyAcknowledgedTheLog() {
        var leader = electedLeader();
        var reply = leader.submit("survives");
        leader.acknowledge(leader.lastAppend("b"), 1, true);
        assertThat(reply).isCompleted();
        var restarted = new NodeFixture("b", "c");
        synchronize(leader, restarted);
        assertThat(restarted.log.snapshot()).isEqualTo(leader.log.snapshot());
    }

    @Test
    void delayedFailureCannotUndoNewerSuccessfulReplication() {
        var leader = electedLeader();
        var old = leader.lastAppend("b");
        var reply = leader.submit("newer");
        leader.acknowledge(leader.lastAppend("b"), 1, true);
        int sentBefore = leader.appends.size();
        leader.acknowledge(old, 1, false);
        assertThat(reply).isCompleted();
        assertThat(leader.appends).hasSize(sentBefore);
        assertThat(leader.log.commitIndex()).isEqualTo(2);
    }

    @Test
    void followerNeverCommitsAnUnverifiedSuffixFromAHeartbeat() {
        var follower = new NodeFixture("b", "c");
        follower.append(new AppendEntriesRequest(
            2, "b", 0, 0,
            List.of(new LogEntry(1, 1, "prefix"), new LogEntry(2, 2, "unverified")), 0));
        follower.append(new AppendEntriesRequest(3, "c", 1, 1, List.of(), 2));
        assertThat(follower.log.commitIndex()).isEqualTo(1);
        assertThat(follower.log.lastIndex()).isEqualTo(2);
    }

    @Test
    void newLeaderCommitsInheritedEntriesThroughItsCurrentTermNoOp() {
        var fixture = new NodeFixture("b", "c");
        fixture.append(new AppendEntriesRequest(
            1, "b", 0, 0,
            List.of(new LogEntry(1, 1, "inherited")), 0));
        fixture.timeout();
        fixture.vote("b", 2, true);
        assertThat(fixture.log.lastTerm()).isEqualTo(2);
        assertThat(fixture.log.commitIndex()).isZero();
        fixture.acknowledge(fixture.lastAppend("b"), 2, true);
        assertThat(fixture.log.commitIndex()).isEqualTo(2);
    }

    @Test
    void snapshotsRemainUnchangedAfterLaterEvents() {
        var fixture = new NodeFixture();
        fixture.timeout();
        var previous = fixture.node.snapshot();
        fixture.submit("later");
        assertThat(previous.log().entries()).hasSize(1);
        assertThat(previous.log().commitIndex()).isEqualTo(1);
        assertThat(fixture.node.snapshot().log().entries()).hasSize(2);
        assertThatThrownBy(() -> previous.log().entries().clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    private NodeFixture electedLeader() {
        var fixture = new NodeFixture("b", "c");
        fixture.timeout();
        fixture.vote("b", 1, true);
        return fixture;
    }

    private void synchronize(NodeFixture leader, NodeFixture follower) {
        for (int attempt = 0; attempt < 10; attempt++) {
            var sent = leader.lastAppend("b");
            var response = follower.append(sent.request());
            leader.acknowledge(sent, response.term(), response.success());
            if (response.success()) {
                follower.append(leader.lastAppend("b").request());
                return;
            }
        }
        throw new AssertionError("Follower did not converge");
    }
}
