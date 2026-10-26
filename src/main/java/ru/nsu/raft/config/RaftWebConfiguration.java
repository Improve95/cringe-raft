package ru.nsu.raft.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.AsyncSupportConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration(proxyBeanMethods = false)
public class RaftWebConfiguration implements WebMvcConfigurer {
    private final ClusterProperties properties;

    public RaftWebConfiguration(ClusterProperties properties) {
        this.properties = properties;
    }

    @Override
    public void configureAsyncSupport(AsyncSupportConfigurer configurer) {
        configurer.setDefaultTimeout(properties.rpcTimeout().plusSeconds(1).toMillis());
    }
}
