package etl.server.service.staging.impl.job;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;


public class DatasetPropertiesTest {
    @Test
    public void createPipelineProperties(){
        DatasetConf result = new DatasetConf();
        result.setLocalPath("input");
        result.setRemotePath("output");
        assertThat(result.getLocalPath()).isEqualTo("input");
        assertThat(result.getRemotePath()).isEqualTo("output");
    }
}