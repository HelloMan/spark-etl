package etl.server;

import etl.common.annotation.ExcludeFromTest;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication
@EnableJpaRepositories
@ExcludeFromTest
public class IgnisApplication {
	public static void main(String[] args) { //NOSONAR
		SpringApplication.run(IgnisApplication.class, args);
	}
}