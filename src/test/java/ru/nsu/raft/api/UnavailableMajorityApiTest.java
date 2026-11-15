package ru.nsu.raft.api;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;
import ru.nsu.raft.node.NodeRole;
import ru.nsu.raft.node.RaftNode;
import ru.nsu.raft.node.event.ElectionTimeout;
import ru.nsu.raft.node.event.VoteResponseReceived;
import ru.nsu.raft.rpc.RequestVoteResponse;
import ru.nsu.raft.runtime.ElectionTimer;
import ru.nsu.raft.runtime.EventQueue;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(properties = {"raft.election-timeout-min=30s", "raft.election-timeout-max=30s",
    "RAFT_CONFIG=classpath:single-node.yaml", "raft.rpc-timeout=100ms",
    "raft.nodes.b=http://127.0.0.1:1", "raft.nodes.c=http://127.0.0.1:1"})
@AutoConfigureMockMvc
@DirtiesContext
class UnavailableMajorityApiTest {
    @Autowired
    private MockMvc mvc;
    @Autowired
    private EventQueue queue;
    @Autowired
    private ElectionTimer timer;
    @Autowired
    private RaftNode node;

    @Test
    void returns503WhenEntryCannotReachAMajority() throws Exception {
        await().atMost(Duration.ofSeconds(2)).until(() -> timer.epoch() > 0);
        queue.post(new ElectionTimeout(timer.epoch()));
        queue.post(new VoteResponseReceived("b", new RequestVoteResponse(1, true)));
        await().atMost(Duration.ofSeconds(2)).until(() -> node.snapshot().status().role() == NodeRole.LEADER);
        mvc.perform(post("/entries").contentType("application/json").content("{\"command\":\"pending\"}"))
            .andExpect(request().asyncNotStarted())
            .andExpect(status().isServiceUnavailable());
        var log = node.snapshot().log();
        assertThat(log.commitIndex()).isZero();
        assertThat(log.entries().getLast().command()).isEqualTo("pending");
    }
}
