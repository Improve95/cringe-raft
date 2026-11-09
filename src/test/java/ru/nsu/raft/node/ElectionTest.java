package ru.nsu.raft.node;

import org.junit.jupiter.api.Test;
import ru.nsu.raft.node.event.ElectionTimeout;
import ru.nsu.raft.node.event.HeartbeatTick;
import ru.nsu.raft.rpc.AppendEntriesRequest;
import ru.nsu.raft.support.NodeFixture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class ElectionTest {
    @Test
    void leaderSendsHeartbeatsAndStopsSendingAfterSteppingDown() {
        var fixture = new NodeFixture("b", "c");
        fixture.timeout();
        fixture.vote("b", 1, true);
        assertThat(fixture.appends).hasSize(2);
        assertThat(fixture.lastAppend("b").request()).isEqualTo(new AppendEntriesRequest(1, "a"));
        fixture.node.handle(new HeartbeatTick());
        assertThat(fixture.appends).hasSize(4);

        fixture.vote("b", 2, false);
        verify(fixture.heartbeat).stop();
        fixture.node.handle(new HeartbeatTick());
        assertThat(fixture.appends).hasSize(4);
        assertThat(fixture.node.nodeStatus().role()).isEqualTo(NodeRole.FOLLOWER);
    }

    @Test
    void followerHeartbeatResetsTimeoutAndRemembersLeader() {
        var fixture = new NodeFixture("b", "c");
        long previousEpoch = fixture.timer.epoch();
        assertThat(fixture.append(new AppendEntriesRequest(2, "b")).success()).isTrue();
        assertThat(fixture.timer.epoch()).isGreaterThan(previousEpoch);
        fixture.node.handle(new ElectionTimeout(previousEpoch));
        assertThat(fixture.node.nodeStatus()).isEqualTo(new NodeStatus("a", NodeRole.FOLLOWER, 2, "b"));
        long epoch = fixture.timer.epoch();
        fixture.append(new AppendEntriesRequest(2, "b"));
        assertThat(fixture.timer.epoch()).isGreaterThan(epoch);
    }

    @Test
    void startsAsFollowerAndSingleNodeElectsItself() {
        var fixture = new NodeFixture();
        assertThat(fixture.node.nodeStatus()).isEqualTo(new NodeStatus("a", NodeRole.FOLLOWER, 0, null));
        fixture.timeout();
        assertThat(fixture.node.nodeStatus()).isEqualTo(new NodeStatus("a", NodeRole.LEADER, 1, "a"));
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
        assertThat(fixture.node.nodeStatus().role()).isEqualTo(NodeRole.CANDIDATE);
        fixture.vote("c", 1, true);
        assertThat(fixture.node.nodeStatus().role()).isEqualTo(NodeRole.LEADER);
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
        assertThat(fixture.node.nodeStatus().role()).isEqualTo(NodeRole.CANDIDATE);
        fixture.vote("b", 2, true);
        assertThat(fixture.node.nodeStatus().role()).isEqualTo(NodeRole.LEADER);
    }

    @Test
    void grantsOneVotePerTermAndRejectsOlderTerms() {
        var fixture = new NodeFixture("b", "c");
        assertThat(fixture.requestVote(3, "b").voteGranted()).isTrue();
        assertThat(fixture.requestVote(3, "b").voteGranted()).isTrue();
        assertThat(fixture.requestVote(3, "c").voteGranted()).isFalse();
        assertThat(fixture.requestVote(2, "c").voteGranted()).isFalse();
        assertThat(fixture.requestVote(4, "c").voteGranted()).isTrue();
    }

    @Test
    void candidateAcceptsSameTermLeaderAndPreservesItsVote() {
        var fixture = new NodeFixture("b", "c");
        fixture.timeout();
        long oldEpoch = fixture.timer.epoch();
        assertThat(fixture.append(new AppendEntriesRequest(1, "b")).success()).isTrue();
        assertThat(fixture.node.nodeStatus()).isEqualTo(new NodeStatus("a", NodeRole.FOLLOWER, 1, "b"));
        assertThat(fixture.requestVote(1, "c").voteGranted()).isFalse();
        fixture.node.handle(new ElectionTimeout(oldEpoch));
        assertThat(fixture.node.nodeStatus().role()).isEqualTo(NodeRole.FOLLOWER);
    }

    @Test
    void oldHeartbeatDoesNotPostponeElection() {
        var fixture = new NodeFixture("b", "c");
        fixture.requestVote(2, "b");
        long epoch = fixture.timer.epoch();
        assertThat(fixture.append(new AppendEntriesRequest(1, "b")).success()).isFalse();
        assertThat(fixture.timer.epoch()).isEqualTo(epoch);
        fixture.timeout();
        assertThat(fixture.node.term()).isEqualTo(3);
    }
}
