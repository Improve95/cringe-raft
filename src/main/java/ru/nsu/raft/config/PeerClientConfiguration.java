package ru.nsu.raft.config;

import java.net.http.HttpClient;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(PeerClientProperties.class)
public class PeerClientConfiguration {
    @Bean(destroyMethod = "shutdownNow")
    public HttpClient peerHttpClient(PeerClientProperties properties) {
        return HttpClient.newBuilder().connectTimeout(properties.connectTimeout()).build();
    }

    @Bean
    public RestClient peerRestClient(RestClient.Builder builder, HttpClient peerHttpClient,
            PeerClientProperties properties) {
        var factory = new JdkClientHttpRequestFactory(peerHttpClient);
        factory.setReadTimeout(properties.readTimeout());
        return builder.requestFactory(factory).build();
    }
}
