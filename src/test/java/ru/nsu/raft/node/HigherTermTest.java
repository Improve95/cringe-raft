package ru.nsu.raft.node;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import ru.nsu.raft.node.event.AppendEntriesResponseReceived;
import ru.nsu.raft.rpc.AppendEntriesRequest;
import ru.nsu.raft.rpc.AppendEntriesResponse;
import ru.nsu.raft.support.NodeFixture;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class HigherTermTest {
    @ParameterizedTest
    @EnumSource(NodeRole.class)
    void higherTermVoteRequestForcesFollower(NodeRole role) {
        var fixture = fixtureIn(role);
        assertThat(fixture.requestVote(5, "c", fixture.log.lastIndex(), fixture.log.lastTerm()).voteGranted()).isTrue();
        assertFollowerInTermFive(fixture);
    }

    @ParameterizedTest
    @EnumSource(NodeRole.class)
    void higherTermAppendRequestForcesFollower(NodeRole role) {
        var fixture = fixtureIn(role);
        fixture.append(new AppendEntriesRequest(5, "b", 0, 0, List.of(), 0));
        assertFollowerInTermFive(fixture);
        assertThat(fixture.node.snapshot().status().leaderId()).isEqualTo("b");
    }

    @ParameterizedTest
    @EnumSource(NodeRole.class)
    void higherTermVoteResponseForcesFollowerEvenWhenVoteIsDenied(NodeRole role) {
        var fixture = fixtureIn(role);
        fixture.vote("b", 5, false);
        assertFollowerInTermFive(fixture);
    }

    @ParameterizedTest
    @EnumSource(NodeRole.class)
    void higherTermAppendResponseForcesFollowerEvenForAnOldRequest(NodeRole role) {
        var fixture = fixtureIn(role);
        var oldRequest = new AppendEntriesRequest(0, "a", 0, 0, List.of(), 0);
        fixture.node.handle(new AppendEntriesResponseReceived(
            "b", 1, oldRequest,
            new AppendEntriesResponse(5, false)));
        assertFollowerInTermFive(fixture);
    }

    private NodeFixture fixtureIn(NodeRole role) {
        var fixture = new NodeFixture("b", "c");
        if (role != NodeRole.FOLLOWER) {
            fixture.timeout();
        }
        if (role == NodeRole.LEADER) {
            fixture.vote("b", 1, true);
        }
        return fixture;
    }

    private void assertFollowerInTermFive(NodeFixture fixture) {
        assertThat(fixture.node.snapshot().status().role()).isEqualTo(NodeRole.FOLLOWER);
        assertThat(fixture.node.term()).isEqualTo(5);
        assertThat(fixture.timer.isCurrent(fixture.timer.epoch())).isTrue();
    }
}
