package ru.nsu.raft.api;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
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
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(properties = {"raft.election-timeout-min=30s", "raft.election-timeout-max=30s",
    "RAFT_CONFIG=classpath:single-node.yaml", "spring.config.import=classpath:transport-test.yaml"})
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
    void servesStatusAndHandlesElectionRpcs() throws Exception {
        await().atMost(Duration.ofSeconds(2)).until(() -> timer.epoch() > 0);
        mvc.perform(get("/status")).andExpect(status().isOk())
            .andExpect(jsonPath("$.nodeId").value("node-1"))
            .andExpect(jsonPath("$.role").value("FOLLOWER"))
            .andExpect(jsonPath("$.term").value(0))
            .andExpect(jsonPath("$.leaderId").value(nullValue()));
        assertThat(transport.connectTimeout()).isEqualTo(Duration.ofMillis(250));
        assertThat(transport.readTimeout()).isEqualTo(Duration.ofMillis(750));

        queue.post(new ElectionTimeout(timer.epoch()));
        await().atMost(Duration.ofSeconds(2)).until(() -> node.nodeStatus().role() == NodeRole.LEADER);
        postJson("/raft/request-vote", "{\"term\":0,\"candidateId\":\"peer\"}")
            .andExpect(status().isOk()).andExpect(jsonPath("$.voteGranted").value(false));
        postJson("/raft/append-entries", "{\"term\":2,\"leaderId\":\"peer\"}")
            .andExpect(status().isOk()).andExpect(jsonPath("$.success").value(true));
        postJson("/raft/append-entries", "{\"term\":-1,\"leaderId\":\"peer\"}")
            .andExpect(status().isBadRequest());
        postJson("/raft/request-vote", "{\"term\":2,\"candidateId\":\"\"}")
            .andExpect(status().isBadRequest());
        mvc.perform(get("/status")).andExpect(jsonPath("$.role").value("FOLLOWER"))
            .andExpect(jsonPath("$.term").value(2)).andExpect(jsonPath("$.leaderId").value("peer"));
    }

    @Test
    void logAndCommandEndpointsAreAbsent() throws Exception {
        mvc.perform(get("/log")).andExpect(status().isNotFound());
        postJson("/entries", "{\"command\":\"hello\"}").andExpect(status().isNotFound());
    }

    private ResultActions postJson(String path, String body) throws Exception {
        return mvc.perform(post(path).contentType("application/json").content(body))
            .andExpect(request().asyncNotStarted());
    }
}
