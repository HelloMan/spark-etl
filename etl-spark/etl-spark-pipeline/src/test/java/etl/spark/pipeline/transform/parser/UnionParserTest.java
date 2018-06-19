package etl.spark.pipeline.transform.parser;

import etl.api.job.pipeline.PipelineJobRequest;
import etl.api.job.pipeline.PipelineStep;
import etl.api.pipeline.SourceLoader;
import etl.api.pipeline.DatasetRefs;
import etl.api.pipeline.Union;
import etl.spark.TestPipelineApplication;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static org.assertj.core.api.Assertions.assertThat;
@RunWith(SpringRunner.class)
@SpringBootTest(classes = TestPipelineApplication.class)
public class UnionParserTest extends AbstractTransformTest {

    @Test
    public void union() throws Exception {
        //arrange
        SourceLoader employee = loadEmployee();

        Union union = Union.builder()
                .name("union")
                .input(DatasetRefs.fromTransform(employee))
                .input(DatasetRefs.fromTransform(employee))
                .output(DatasetRefs.datasetRef("result"))
                .build();


        PipelineJobRequest pipeline = PipelineJobRequest.builder()
                .name("unionTest")
                .step(PipelineStep.builder()
                        .name("step")
                        .transform(employee)
                        .transform(union)
                        .build())
                .build();
        getJobOperator().runJob(pipeline);
       //assert
       long count = driverContext.getSparkSession().sql("select * from result").count();
       assertThat(count).isEqualTo(10);

    }
}
