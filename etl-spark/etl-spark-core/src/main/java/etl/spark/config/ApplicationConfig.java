package etl.spark.config;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.Scope;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@PropertySource("classpath:${job.properties.file}")
@Configuration
@Slf4j
public class ApplicationConfig {

	@Value("${debug.mode}")
	@Setter(AccessLevel.PACKAGE)
	private boolean debugMode;


	@Bean
	@Scope(ConfigurableBeanFactory.SCOPE_SINGLETON)
	public SparkSession sparkSession() {
		if (debugMode) {
			String target = new File("target").getAbsolutePath();
			String sparkWarehouse = target + "/spark-warehouse";
			String hiveMetaStore = String.format("jdbc:derby:;databaseName=%s/metastore_db;create=true", target);

			return SparkSession.builder()
					.config("spark.sql.warehouse.dir", sparkWarehouse)
					.config("javax.jdo.option.ConnectionURL", hiveMetaStore)
					.master("local[*]")
					.enableHiveSupport()
					.getOrCreate();
		} else {
			return SparkSession.builder().getOrCreate();
		}
	}

	@Bean
	@Scope(ConfigurableBeanFactory.SCOPE_SINGLETON)
	public JavaSparkContext javaSparkContext(SparkSession sparkSession) {
		return new JavaSparkContext(sparkSession.sparkContext());
	}


}
