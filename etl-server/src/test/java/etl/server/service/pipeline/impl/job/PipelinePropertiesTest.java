package etl.server.service.pipeline.impl.job;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
public class PipelinePropertiesTest {

    @Test
    public void createPipelineProperties(){
        PipelineConf pipelineConf = new PipelineConf();
        pipelineConf.setStagingPath("stagingPath");
        assertThat(pipelineConf.getStagingPath()).isEqualTo("stagingPath");
    }

}