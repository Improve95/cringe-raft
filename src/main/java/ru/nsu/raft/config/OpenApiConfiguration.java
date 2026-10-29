package ru.nsu.raft.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class OpenApiConfiguration {
    @Bean
    OpenAPI raftOpenApi() {
        return new OpenAPI().info(new Info()
            .title("Raft API")
            .version("1.0")
            .description("Выборы лидера и репликация лога в учебном кластере Raft."));
    }
}
