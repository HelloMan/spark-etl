package etl.server.config;

import etl.server.service.pipeline.impl.job.PipelineConf;
import etl.server.service.staging.impl.job.DatasetConf;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * entry point to import properties configuration class
 */
@Configuration
@EnableConfigurationProperties({DatasetConf.class, PipelineConf.class})
public class ApplicationConf {


}
