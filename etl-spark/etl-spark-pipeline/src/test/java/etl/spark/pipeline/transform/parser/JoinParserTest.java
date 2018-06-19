package etl.spark.pipeline.transform.parser;

import etl.api.pipeline.Join;
import etl.api.pipeline.JoinField;
import etl.api.pipeline.JoinOn;
import etl.api.pipeline.JoinType;
import etl.api.job.pipeline.PipelineJobRequest;
import etl.api.job.pipeline.PipelineStep;
import etl.api.pipeline.SourceLoader;
import etl.api.pipeline.DatasetRefs;
import etl.spark.TestPipelineApplication;
import etl.spark.pipeline.transform.TransformUtil;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static org.assertj.core.api.Assertions.assertThat;
@RunWith(SpringRunner.class)
@SpringBootTest(classes = TestPipelineApplication.class)
public class JoinParserTest extends AbstractTransformTest {

    @Test
    public void joinParser() throws Exception {
        //arrange
        SourceLoader leftSource = SourceLoader.builder().name("emp")
                .source(TransformUtil.getCsvSource("data/emp.csv"))
                .build();

        SourceLoader rightSource  = SourceLoader.builder()
                .name("dept")
                .source(TransformUtil.getCsvSource("data/dept.csv"))
                .build();

        Join join = Join.builder().name("join")
                .left(DatasetRefs.fromTransform(leftSource))
                .right(DatasetRefs.fromTransform(rightSource))
                .joinType(JoinType.INNER)
                .field(JoinField.left("name"))
                .field(JoinField.builder().name("name").as("deptNmae").build())
                .on(JoinOn.on("deptId", "id"))
                .output(DatasetRefs.datasetRef("joinResult"))
                .build();

        PipelineJobRequest pipeline = PipelineJobRequest.builder()
                .name("joinTest")
                .step(PipelineStep.builder()
                        .name("step")
                        .transform(leftSource)
                        .transform(rightSource)
                        .transform(join)
                        .build())
                .build();

        getJobOperator().runJob(pipeline);

        //assert
        Assertions.assertThat(driverContext.getSparkSession().sql("select * from joinResult").count()).isEqualTo(5);


    }
}
