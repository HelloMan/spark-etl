package etl.spark.pipeline.transform.parser;

import etl.api.job.pipeline.PipelineJobRequest;
import etl.api.job.pipeline.PipelineStep;
import etl.api.pipeline.DatasetRef;
import etl.api.pipeline.DatasetRefs;
import etl.api.pipeline.Distinct;
import etl.api.pipeline.SourceLoader;
import etl.spark.TestPipelineApplication;
import etl.spark.util.DatasetRow;
import org.apache.spark.sql.Row;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
@RunWith(SpringRunner.class)
@SpringBootTest(classes = TestPipelineApplication.class)
public class DistinctParserTest extends AbstractTransformTest {

    @Test
    public void distinctWithFields() throws Exception {
        //arrange
        SourceLoader employee = loadEmployee();

        Distinct distinct = Distinct.builder()
                .name("distinct")
                .input(DatasetRefs.fromTransform(employee))
                .field("deptId")
                .output(DatasetRefs.datasetRef("result"))
                .build();


        PipelineJobRequest pipeline = PipelineJobRequest.builder()
                .name("joinTest")
                .step(PipelineStep.builder()
                        .name("joinStep")
                        .transform(employee)
                        .transform(distinct)
                        .build())
                .build();

        //act
		getJobOperator().runJob(pipeline);
		//assert
        long count = driverContext.getSparkSession().sql("select * from result").count();
        assertThat(count).isEqualTo(3);


    }
    @Test
    public void distinctWithoutFields() throws Exception {
        //arrange
        SourceLoader employee = loadEmployee();
        Distinct distinctWithoutField = Distinct.builder()
                .name("distinctWithoutField")
                .input(DatasetRefs.fromTransform(employee))
                .output(DatasetRefs.datasetRef("result"))
                .build();

        PipelineJobRequest pipeline = PipelineJobRequest.builder()
                .name("distinctTest")
                .step(PipelineStep.builder()
                        .name("step")
                       .transform(employee)
                       .transform(distinctWithoutField)
                       .build())
                .build();

		getJobOperator().runJob(pipeline);
        //assert
        long count = driverContext.getSparkSession().sql("select * from result").count();
        assertThat(count).isEqualTo(5);



    }

	@Test
	public void distinctWithIncludeFields() throws Exception {
		//arrange
		SourceLoader employee = loadEmployee();
		Distinct distinct = Distinct.builder()
				.name("distinctWithIncludeFields")
				.input(DatasetRefs.fromTransform(employee))
				.output(DatasetRef.builder().name("result").include("deptId").build())
				.build();

		PipelineJobRequest pipeline = PipelineJobRequest.builder()
				.name("distinctTest")
				.step(PipelineStep.builder()
						.name("step")
						.transform(employee)
						.transform(distinct)
						.build())
				.build();

		getJobOperator().runJob(pipeline);
		List<Row> rows = driverContext.getSparkSession().sql("select * from result").collectAsList();
		assertThat(new DatasetRow (rows.get(0)).asMap().size()).isEqualTo(1);
	}

	@Test
	public void distinctWithExcludeFields() throws Exception {
		//arrange
		SourceLoader employee = loadEmployee();
		Distinct distinct = Distinct.builder()
				.name("distinctWithIncludeFields")
				.input(DatasetRefs.fromTransform(employee))
				.output(DatasetRef.builder().name("result").exclude("deptId").build())
				.build();

		PipelineJobRequest pipeline = PipelineJobRequest.builder()
				.name("distinctTest")
				.step(PipelineStep.builder()
						.name("step")
						.transform(employee)
						.transform(distinct)
						.build())
				.build();

		getJobOperator().runJob(pipeline);
		List<Row> rows = driverContext.getSparkSession().sql("select * from result").collectAsList();
		assertThat(new DatasetRow(rows.get(0)).asMap().size()).isEqualTo(3);
	}

}
