package etl.spark.pipeline.transform.parser;

import etl.api.job.pipeline.PipelineJobRequest;
import etl.api.job.pipeline.PipelineStep;
import etl.api.pipeline.SourceLoader;
import etl.api.pipeline.Split;
import etl.api.pipeline.DatasetRefs;
import etl.spark.TestPipelineApplication;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static org.assertj.core.api.Assertions.assertThat;
@RunWith(SpringRunner.class)
@SpringBootTest(classes = TestPipelineApplication.class)
public class SplitParserTest extends AbstractTransformTest {

    @Test
    public void split() throws Exception {
        //arrange
        SourceLoader employee = loadEmployee();
        Split split = Split.builder()
                .name("split")
                .input(DatasetRefs.fromTransform(employee))
                .condition("salary>2500")
                .output1(DatasetRefs.datasetRef("salaryGreaterThan2500"))
                .output2(DatasetRefs.datasetRef("salaryLessOrEqualThan2500"))
                .build();

        PipelineJobRequest pipeline = PipelineJobRequest.builder()
                .name("splitTest")
                .step(PipelineStep.builder()
                        .name("step")
                        .transform(employee)
                        .transform(split)
                        .build())
                .build();

        getJobOperator().runJob(pipeline);
        //assert
        long salaryGreaterThan2500 = driverContext.getSparkSession().sql("select * from salaryGreaterThan2500 ").count();
        long salaryLessOrEqualThan2500 = driverContext.getSparkSession().sql("select * from salaryLessOrEqualThan2500 ").count();
        assertThat(salaryGreaterThan2500).isEqualTo(3);
        assertThat(salaryLessOrEqualThan2500).isEqualTo(2);

    }
}
