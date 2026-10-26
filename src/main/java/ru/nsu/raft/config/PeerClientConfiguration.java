package ru.nsu.raft.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;
import ru.nsu.raft.runtime.EventQueue;
import ru.nsu.raft.transport.PeerClient;

import java.net.http.HttpClient;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(PeerClientProperties.class)
public class PeerClientConfiguration {
    @Bean(destroyMethod = "shutdownNow")
    ExecutorService peerExecutor() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }

    @Bean(destroyMethod = "shutdownNow")
    HttpClient peerHttpClient(PeerClientProperties properties) {
        return HttpClient.newBuilder().connectTimeout(properties.connectTimeout())
            .version(HttpClient.Version.HTTP_1_1).build();
    }

    @Bean
    RestClient peerRestClient(HttpClient peerHttpClient, PeerClientProperties properties,
                              @Qualifier("peerExecutor") ExecutorService executor) {
        var requestFactory = new JdkClientHttpRequestFactory(peerHttpClient, executor);
        requestFactory.setReadTimeout(properties.readTimeout());
        return RestClient.builder().requestFactory(requestFactory).build();
    }

    @Bean
    PeerClient peerClient(RestClient peerRestClient, @Qualifier("peerExecutor") ExecutorService executor,
                          EventQueue queue) {
        return new PeerClient(peerRestClient, executor, queue);
    }
}
