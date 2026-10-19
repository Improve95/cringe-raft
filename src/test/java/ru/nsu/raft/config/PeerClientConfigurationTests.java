package ru.nsu.raft.config;

import java.net.http.HttpClient;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.web.client.RestClient;
import static org.assertj.core.api.Assertions.assertThat;

class PeerClientConfigurationTests {
    private final ApplicationContextRunner context = new ApplicationContextRunner()
            .withInitializer(new ConfigDataApplicationContextInitializer())
            .withPropertyValues("spring.config.location=classpath:transport-test.yaml")
            .withBean(RestClient.Builder.class, RestClient::builder)
            .withUserConfiguration(PeerClientConfiguration.class);

    @Test void bindsYamlAndCreatesClientBeans() {
        context.run(application -> {
            assertThat(application).hasNotFailed().hasSingleBean(RestClient.class);
            var properties = application.getBean(PeerClientProperties.class);
            assertThat(properties.connectTimeout()).isEqualTo(Duration.ofMillis(250));
            assertThat(properties.readTimeout()).isEqualTo(Duration.ofMillis(750));
            assertThat(application.getBean(HttpClient.class).connectTimeout())
                    .contains(Duration.ofMillis(250));
        });
    }

    @Test void rejectsNonPositiveTimeout() {
        context.withPropertyValues("raft.transport.read-timeout=0ms").run(application ->
                assertThat(application).hasFailed());
    }
}
