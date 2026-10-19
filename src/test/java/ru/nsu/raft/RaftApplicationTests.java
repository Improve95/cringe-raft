package ru.nsu.raft;

import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import tools.jackson.databind.json.JsonMapper;
import static org.junit.jupiter.api.Assertions.*;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class RaftApplicationTests {
    @Value("${local.server.port}")
    private int port;
    private final JsonMapper mapper = new JsonMapper();

    @Test void electsLeaderAndServesAsyncRpcAndLogApi() throws Exception {
        try (var client = HttpClient.newHttpClient()) {
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                var status = mapper.readTree(send(client, "/status", null).body());
                assertEquals("LEADER", status.get("role").asText());
                assertEquals("node-1", status.get("leaderId").asText());
            });
            var response = send(client, "/entries", "{\"command\":\"hello\"}");
            assertEquals(200, response.statusCode());
            long index = mapper.readTree(response.body()).get("index").asLong();
            await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
                var log = mapper.readTree(send(client, "/log", null).body());
                assertEquals(index, log.get("commitIndex").asLong());
                assertEquals("hello", log.get("entries").get((int) index - 1).get("command").asText());
            });
            assertEquals(400, send(client, "/entries", "{\"command\":\"\"}").statusCode());
            assertEquals(400, send(client, "/raft/request-vote", "{\"term\":-1}").statusCode());
            var rejected = send(client, "/raft/request-vote",
                    "{\"term\":0,\"candidateId\":\"old\",\"lastLogIndex\":0,\"lastLogTerm\":0}");
            assertEquals(200, rejected.statusCode());
            assertFalse(mapper.readTree(rejected.body()).get("voteGranted").asBoolean());
            var heartbeat = send(client, "/raft/append-entries",
                    "{\"term\":0,\"leaderId\":\"old\",\"prevLogIndex\":0,\"prevLogTerm\":0,\"entries\":[],\"leaderCommit\":0}");
            assertEquals(200, heartbeat.statusCode());
            assertFalse(mapper.readTree(heartbeat.body()).get("success").asBoolean());
        }
    }
    private HttpResponse<String> send(HttpClient client, String path, String body) throws Exception {
        var builder = HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                .timeout(Duration.ofSeconds(3));
        if (body != null) builder.header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(body));
        return client.send(builder.build(), HttpResponse.BodyHandlers.ofString());
    }
}
