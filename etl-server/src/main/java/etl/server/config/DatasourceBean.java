package etl.server.config;

import etl.server.annotation.DataSourceQualifier;
import etl.common.annotation.ExcludeFromTest;
import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
@ExcludeFromTest
@Configuration
public class DatasourceBean {

	@Bean
	@Primary
	@DataSourceQualifier(DataSourceQualifier.Target.MASTER)
	@ConfigurationProperties(prefix="spring.datasource")
	public DataSource defaultDataSource() {
		return DataSourceBuilder.create().build();
	}

	@Bean
	@DataSourceQualifier(DataSourceQualifier.Target.PHOENIX)
	@ConfigurationProperties(prefix="phoenix.datasource")
	public DataSource hiveDataSource() {
		return DataSourceBuilder.create().build();
	}

	@Bean
	@DataSourceQualifier(DataSourceQualifier.Target.MASTER)
	@Primary
	public JdbcTemplate jdbcTemplate(@DataSourceQualifier(DataSourceQualifier.Target.MASTER) DataSource dataSource) {
		return new JdbcTemplate(dataSource);
	}

	@Bean
	@DataSourceQualifier(DataSourceQualifier.Target.PHOENIX)
	public JdbcTemplate phoenixJdbcTemplate(@DataSourceQualifier(DataSourceQualifier.Target.PHOENIX) DataSource dataSource) {
		return new JdbcTemplate(dataSource);
	}

}