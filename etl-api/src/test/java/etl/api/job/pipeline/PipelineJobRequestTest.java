package etl.api.job.pipeline;

import etl.api.AbstractJavaBeanTest;
import etl.api.parameter.IntParameter;
import etl.api.pipeline.FieldType;
import etl.api.pipeline.Map;
import etl.api.pipeline.MapField;
import etl.api.pipeline.DatasetRefs;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class PipelineJobRequestTest extends AbstractJavaBeanTest<PipelineJobRequest> {

    @Test
    public void testBuilder() {

        PipelineJobRequest pipeline = createPipeline();

        assertThat(pipeline.getName()).isEqualTo("pipeline");
        assertThat(pipeline.getSteps().size()).isEqualTo(1);

        PipelineStep pipelineStep = pipeline.getSteps().iterator().next();

        assertThat(pipelineStep.getName()).isEqualTo("step1");
        assertThat(pipelineStep.getTransforms().size()).isEqualTo(1);


    }

	@Test
	public void testGetEmptyParameters() {
		PipelineJobRequest job = createPipeline();
		Assertions.assertThat(job.getParameters().isEmpty()).isTrue();
	}

	@Test
	public void testGetNotEmptyParameters() {
		PipelineJobRequest job = PipelineJobRequest.builder()
				.name("pipeline")
				.step(createPipelineStep())
				.jobParameter(new IntParameter("param", 1))
				.build();
		Assertions.assertThat(job.getParameters().isEmpty()).isFalse();
		Assertions.assertThat(job.getParameters().getInt("param")).isEqualTo(1);
	}

    private PipelineJobRequest createPipeline() {
        return PipelineJobRequest.builder()
                .name("pipeline")
                .step(createPipelineStep())
                .build();
    }

    private PipelineStep createPipelineStep() {
        return PipelineStep.builder()
                .name("step1")
                .description("desc")
                .transform(createMap())

                .build();

    }

    private Map createMap() {
        return Map.builder()
                .name("map")
                .field(MapField.builder().name("age")
                        .fieldType(FieldType.of(FieldType.Type.INT))
                        .build())
                .input(DatasetRefs.datasetRef("employee"))
                .output(DatasetRefs.datasetRef("mapOutput"))
                .build();
    }



}