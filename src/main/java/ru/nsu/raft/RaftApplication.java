package ru.nsu.raft;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import ru.nsu.raft.config.ClusterProperties;

@SpringBootApplication
@EnableConfigurationProperties(ClusterProperties.class)
public class RaftApplication {

	public static void main(String[] args) {
		SpringApplication.run(RaftApplication.class, args);
	}
}
