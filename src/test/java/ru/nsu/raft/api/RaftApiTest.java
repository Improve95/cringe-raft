package ru.nsu.raft.api;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import ru.nsu.raft.config.PeerClientProperties;
import ru.nsu.raft.node.NodeRole;
import ru.nsu.raft.node.RaftNode;
import ru.nsu.raft.node.event.ElectionTimeout;
import ru.nsu.raft.runtime.ElectionTimer;
import ru.nsu.raft.runtime.EventQueue;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.nullValue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(properties = {"raft.election-timeout-min=30s", "raft.election-timeout-max=30s",
    "spring.config.import=classpath:transport-test.yaml"})
@AutoConfigureMockMvc
@DirtiesContext
class RaftApiTest {
    @Autowired
    private MockMvc mvc;
    @Autowired
    private EventQueue queue;
    @Autowired
    private ElectionTimer timer;
    @Autowired
    private RaftNode node;
    @Autowired
    private PeerClientProperties transport;

    @Test
    void servesSnapshotsAndHandlesCommandsAndRpcsAsynchronously() throws Exception {
        await().atMost(Duration.ofSeconds(2)).until(() -> timer.epoch() > 0);
        mvc.perform(get("/status")).andExpect(status().isOk())
            .andExpect(jsonPath("$.nodeId").value("node-1"))
            .andExpect(jsonPath("$.role").value("FOLLOWER"))
            .andExpect(jsonPath("$.term").value(0))
            .andExpect(jsonPath("$.leaderId").value(nullValue()));
        assertThat(transport.connectTimeout()).isEqualTo(Duration.ofMillis(250));
        assertThat(transport.readTimeout()).isEqualTo(Duration.ofMillis(750));
        mvc.perform(post("/entries").contentType("application/json").content("{\"command\":\" \"}"))
            .andExpect(status().isBadRequest());
        mvc.perform(asyncDispatch(postAsync("/entries", "{\"command\":\"wrong-role\"}")))
            .andExpect(status().isConflict());

        queue.post(new ElectionTimeout(timer.epoch()));
        await().atMost(Duration.ofSeconds(2)).until(() -> node.snapshot().status().role() == NodeRole.LEADER);
        mvc.perform(asyncDispatch(postAsync("/entries", "{\"command\":\"hello\"}")))
            .andExpect(status().isOk()).andExpect(jsonPath("$.index").value(2))
            .andExpect(jsonPath("$.term").value(1));
        mvc.perform(get("/log")).andExpect(status().isOk()).andExpect(jsonPath("$.commitIndex").value(2))
            .andExpect(jsonPath("$.entries[1].command").value("hello"));

        mvc.perform(asyncDispatch(postAsync(
                "/raft/request-vote",
                "{\"term\":0,\"candidateId\":\"peer\",\"lastLogIndex\":0,\"lastLogTerm\":0}")))
            .andExpect(status().isOk()).andExpect(jsonPath("$.voteGranted").value(false));
        mvc.perform(asyncDispatch(postAsync(
                "/raft/append-entries",
                "{\"term\":2,\"leaderId\":\"peer\",\"prevLogIndex\":2,\"prevLogTerm\":1,"
                + "\"entries\":[],\"leaderCommit\":2}")))
            .andExpect(status().isOk()).andExpect(jsonPath("$.success").value(true));
        mvc.perform(asyncDispatch(postAsync("/entries", "{\"command\":\"redirect\"}")))
            .andExpect(status().isConflict()).andExpect(jsonPath("$.leaderId").value("peer"));
        mvc.perform(post("/raft/append-entries").contentType("application/json")
                .content("{\"term\":2,\"leaderId\":\"peer\",\"prevLogIndex\":0,\"prevLogTerm\":0,"
                         + "\"entries\":[{\"index\":3,\"term\":2,\"command\":\"gap\"}],\"leaderCommit\":0}"))
            .andExpect(status().isBadRequest());
        mvc.perform(get("/status")).andExpect(jsonPath("$.role").value("FOLLOWER"))
            .andExpect(jsonPath("$.term").value(2)).andExpect(jsonPath("$.leaderId").value("peer"));
    }

    private MvcResult postAsync(String path, String body) throws Exception {
        return mvc.perform(post(path).contentType("application/json").content(body))
            .andExpect(request().asyncStarted()).andReturn();
    }
}
