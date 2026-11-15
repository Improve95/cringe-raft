package ru.nsu.raft.api;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;
import org.springframework.http.MediaType;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.web.client.RestClient;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
import ru.nsu.raft.rpc.RequestVoteRequest;
import ru.nsu.raft.rpc.RequestVoteResponse;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {"raft.election-timeout-min=30s", "raft.election-timeout-max=30s",
        "RAFT_CONFIG=classpath:single-node.yaml"}
)
@Import(VirtualThreadHttpTest.ProbeConfiguration.class)
@DirtiesContext
class VirtualThreadHttpTest {
    @Autowired
    private Environment environment;
    @Autowired
    private RestClient peerRestClient;

    @Test
    void handlesBlockingRpcOnAVirtualServerThread() {
        int port = environment.getRequiredProperty("local.server.port", Integer.class);
        var response = peerRestClient.post()
            .uri("http://localhost:" + port + "/raft/request-vote")
            .contentType(MediaType.APPLICATION_JSON)
            .body(new RequestVoteRequest(1, "peer", 0, 0))
            .retrieve()
            .toEntity(RequestVoteResponse.class);

        assertThat(response.getHeaders().getFirst("X-Test-Virtual-Thread")).isEqualTo("true");
        assertThat(response.getBody()).isEqualTo(new RequestVoteResponse(1, true));
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class ProbeConfiguration {
        @Bean
        WebMvcConfigurer virtualThreadProbe() {
            return new WebMvcConfigurer() {
                @Override
                public void addInterceptors(InterceptorRegistry registry) {
                    registry.addInterceptor(new HandlerInterceptor() {
                        @Override
                        public boolean preHandle(
                            HttpServletRequest request,
                            HttpServletResponse response,
                            Object handler
                        ) {
                            response.setHeader(
                                "X-Test-Virtual-Thread",
                                Boolean.toString(Thread.currentThread().isVirtual()));
                            return true;
                        }
                    });
                }
            };
        }
    }
}
