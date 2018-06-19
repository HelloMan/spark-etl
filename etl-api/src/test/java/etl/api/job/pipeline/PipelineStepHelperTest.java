package etl.api.job.pipeline;

import etl.api.pipeline.DatasetRef;
import etl.api.pipeline.Map;
import etl.api.pipeline.Split;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class PipelineStepHelperTest {

    private PipelineStepHelper pipelineStepHelper;

    @Before
    public void init(){
        PipelineStep pipelineStep = PipelineStep.builder()
                .name("pipeline")
                .transform(Map.builder()
                        .name("map")
                        .input(DatasetRef.builder()
                                .name("mapInput")
                                .build())
                        .build())
                .transform(Split.builder()
                        .name("split")
                        .condition("a>1")
                        .input(DatasetRef.builder()
                                .name("map")
                                .build())
                        .output1(DatasetRef.builder()
                                .name("output1").build())
                        .output2(DatasetRef.builder()
                                .name("output2").build())
                        .build())
                .build();

        pipelineStepHelper = new PipelineStepHelper(pipelineStep);
    }
    @Test
    public void testIsImmediateDatasetRef() throws Exception {

        assertThat(pipelineStepHelper.isImmediateDatasetRef(DatasetRef.builder()
                .name("map")
                .build())).isTrue();
    }

    @Test
    public void testGetImmediateDatasets() throws Exception {

        assertThat(pipelineStepHelper.getImmediateDatasets().size()).isEqualTo(1);
    }

    @Test
    public void testGetPipelineStep() throws Exception {
        assertThat(pipelineStepHelper.getPipelineStep()).isNotNull();
    }
}