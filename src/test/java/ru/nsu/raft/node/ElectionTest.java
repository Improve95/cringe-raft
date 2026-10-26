package ru.nsu.raft.node;

import org.junit.jupiter.api.Test;
import ru.nsu.raft.node.event.ElectionTimeout;
import ru.nsu.raft.rpc.AppendEntriesRequest;
import ru.nsu.raft.support.NodeFixture;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class ElectionTest {
    @Test
    void startsAsFollowerAndSingleNodeElectsItself() {
        var fixture = new NodeFixture();
        assertThat(fixture.node.snapshot().status()).isEqualTo(new NodeStatus("a", NodeRole.FOLLOWER, 0, null));
        fixture.timeout();
        assertThat(fixture.node.snapshot().status()).isEqualTo(new NodeStatus("a", NodeRole.LEADER, 1, "a"));
        assertThat(fixture.node.snapshot().log().commitIndex()).isEqualTo(1);
        verify(fixture.heartbeat).start();
        verify(fixture.client, never()).requestVote(any(), any());
    }

    @Test
    void countsOnlyDistinctGrantedVotesFromCurrentTermAndClusterMembers() {
        var fixture = new NodeFixture("b", "c", "d", "e");
        fixture.timeout();
        fixture.vote("b", 0, true);
        fixture.vote("outsider", 1, true);
        fixture.vote("c", 1, false);
        fixture.vote("b", 1, true);
        fixture.vote("b", 1, true);
        assertThat(fixture.node.snapshot().status().role()).isEqualTo(NodeRole.CANDIDATE);
        fixture.vote("c", 1, true);
        assertThat(fixture.node.snapshot().status().role()).isEqualTo(NodeRole.LEADER);
        assertThat(fixture.appends).hasSize(4);
    }

    @Test
    void splitVoteStartsNewElectionAndDiscardsOldVotesAndTimerEvents() {
        var fixture = new NodeFixture("b", "c");
        long expiredEpoch = fixture.timer.epoch();
        fixture.timeout();
        fixture.node.handle(new ElectionTimeout(expiredEpoch));
        assertThat(fixture.node.term()).isEqualTo(1);
        fixture.timeout();
        assertThat(fixture.node.term()).isEqualTo(2);
        fixture.vote("b", 1, true);
        assertThat(fixture.node.snapshot().status().role()).isEqualTo(NodeRole.CANDIDATE);
        fixture.vote("b", 2, true);
        assertThat(fixture.node.snapshot().status().role()).isEqualTo(NodeRole.LEADER);
    }

    @Test
    void grantsOneVotePerTermAndRequiresAnUpToDateLog() {
        var fixture = new NodeFixture("b", "c");
        fixture.log.append(2, "existing");
        assertThat(fixture.requestVote(3, "b", 5, 1).voteGranted()).isFalse();
        assertThat(fixture.node.term()).isEqualTo(3);
        assertThat(fixture.requestVote(3, "b", 1, 2).voteGranted()).isTrue();
        assertThat(fixture.requestVote(3, "b", 1, 2).voteGranted()).isTrue();
        assertThat(fixture.requestVote(3, "c", 2, 2).voteGranted()).isFalse();
        assertThat(fixture.requestVote(2, "c", 1, 2).voteGranted()).isFalse();
        assertThat(fixture.requestVote(4, "c", 1, 2).voteGranted()).isTrue();
    }

    @Test
    void candidateAcceptsSameTermLeaderAndPreservesItsVote() {
        var fixture = new NodeFixture("b", "c");
        fixture.timeout();
        long oldEpoch = fixture.timer.epoch();
        assertThat(fixture.append(new AppendEntriesRequest(1, "b", 0, 0, List.of(), 0)).success()).isTrue();
        assertThat(fixture.node.snapshot().status()).isEqualTo(new NodeStatus("a", NodeRole.FOLLOWER, 1, "b"));
        assertThat(fixture.requestVote(1, "c", 0, 0).voteGranted()).isFalse();
        fixture.node.handle(new ElectionTimeout(oldEpoch));
        assertThat(fixture.node.snapshot().status().role()).isEqualTo(NodeRole.FOLLOWER);
    }

    @Test
    void oldHeartbeatDoesNotPostponeElection() {
        var fixture = new NodeFixture("b", "c");
        fixture.requestVote(2, "b", 0, 0);
        long epoch = fixture.timer.epoch();
        assertThat(fixture.append(new AppendEntriesRequest(1, "b", 0, 0, List.of(), 0)).success()).isFalse();
        assertThat(fixture.timer.epoch()).isEqualTo(epoch);
        fixture.timeout();
        assertThat(fixture.node.term()).isEqualTo(3);
    }
}
