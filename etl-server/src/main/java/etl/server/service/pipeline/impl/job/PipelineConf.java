package etl.server.service.pipeline.impl.job;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("pipeline")
@Getter
@Setter
public class PipelineConf {

    private String stagingPath;
}
