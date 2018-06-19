package etl.spark.pipeline.transform.parser;

import etl.api.pipeline.AggregateField;
import etl.api.pipeline.Aggregator;
import etl.api.job.pipeline.PipelineJobRequest;
import etl.api.job.pipeline.PipelineStep;
import etl.api.pipeline.SourceLoader;
import etl.api.pipeline.DatasetRefs;
import etl.spark.TestPipelineApplication;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static org.assertj.core.api.Assertions.assertThat;
@RunWith(SpringRunner.class)
@SpringBootTest(classes = TestPipelineApplication.class)
public class AggregatorParserTest extends AbstractTransformTest {

	@Test
	public void aggregatorWithoutGroupBy() throws Exception {
		//arrange
		SourceLoader employee = loadEmployee();
		Aggregator sumSalary = Aggregator.builder()
				.name("aggerator")
				.input(DatasetRefs.fromTransform(employee))
				.aggregator(AggregateField.sum("salary", "sumOfSalary"))
				.output(DatasetRefs.datasetRef("sumSalary"))
				.build();

		PipelineJobRequest pipeline = PipelineJobRequest.builder()
				.name("aggregatorTest")
				.step(PipelineStep.builder()
						.name("step")
						.transform(employee)
						.transform(sumSalary)
						.build())
				.build();

		//act
		getJobOperator().runJob(pipeline);

		//assert
		long sumOfSalary = driverContext.getSparkSession().sql("select * from sumSalary").first().getAs("sumOfSalary");
		assertThat(sumOfSalary).isEqualTo(13600);
	}

	@Test
	public void aggregatorWithGroupBy() throws Exception {
		//arrange
		SourceLoader employee = loadEmployee();
		Aggregator sumSalary = Aggregator.builder()
				.name("aggerator")
				.input(DatasetRefs.fromTransform(employee))
				.aggregator(AggregateField.sum("salary", "sumOfSalary"))
				.groupBy("deptId")
				.output(DatasetRefs.datasetRef("sumSalary"))
				.build();

		PipelineJobRequest pipeline = PipelineJobRequest.builder().name("joinTest").step(
				PipelineStep.builder().name("joinStep")
						.transform(employee)
						.transform(sumSalary)
						.build())
				.build();

		//act
		getJobOperator().runJob(pipeline);

		//assert
		long count = driverContext.getSparkSession().sql("select * from sumSalary").count();
		assertThat(count).isEqualTo(3);
		long sumOfSalary = driverContext.getSparkSession().sql("select * from sumSalary").first().getAs("sumOfSalary");
		assertThat(sumOfSalary).isEqualTo(5000);
	}

	@Test
	public void aggregatorWithGroupBy_avg() throws Exception {
		//arrange
		SourceLoader employee = loadEmployee();
		Aggregator avgSalary = Aggregator.builder()
				.name("aggerator")
				.input(DatasetRefs.fromTransform(employee))
				.aggregator(AggregateField.avg("salary", "avgOfSalary"))
				.groupBy("deptId")
				.output(DatasetRefs.datasetRef("AvgSalary"))
				.build();

		PipelineJobRequest pipeline = PipelineJobRequest.builder().name("agg_avg").step(
				PipelineStep.builder().name("step1")
						.transform(employee)
						.transform(avgSalary)
						.build())
				.build();

		//act
		getJobOperator().runJob(pipeline);

		//assert
		double salary = driverContext.getSparkSession().sql("select * from AvgSalary").first().getAs("avgOfSalary");
		assertThat(salary).isEqualTo(2500);
	}

	@Test
	public void aggregatorWithoutGroupBy_max() throws Exception {
		//arrange
		SourceLoader employee = loadEmployee();
		Aggregator avgSalary = Aggregator.builder()
				.name("aggerator")
				.input(DatasetRefs.fromTransform(employee))
				.aggregator(AggregateField.max("salary", "maxOfSalary"))
				.output(DatasetRefs.datasetRef("MaxSalary"))
				.build();

		PipelineJobRequest pipeline = PipelineJobRequest.builder().name("agg_max").step(
				PipelineStep.builder().name("step1")
						.transform(employee)
						.transform(avgSalary)
						.build())
				.build();


		//act
		getJobOperator().runJob(pipeline);

		//assert
		int salary = driverContext.getSparkSession().sql("select * from MaxSalary").first().getAs("maxOfSalary");
		assertThat(salary).isEqualTo(3300);
	}

	@Test
	public void aggregatorWithoutGroupBy_min() throws Exception {
		//arrange
		SourceLoader employee = loadEmployee();
		Aggregator avgSalary = Aggregator.builder()
				.name("aggerator")
				.input(DatasetRefs.fromTransform(employee))
				.aggregator(AggregateField.min("salary", "minOfSalary"))
				.output(DatasetRefs.datasetRef("MinSalary"))
				.build();

		PipelineJobRequest pipeline = PipelineJobRequest.builder().name("agg_min").step(
				PipelineStep.builder().name("step1")
						.transform(employee)
						.transform(avgSalary)
						.build())
				.build();

		//act
		getJobOperator().runJob(pipeline);

		//assert
		int salary = driverContext.getSparkSession().sql("select * from MinSalary").first().getAs("minOfSalary");
		assertThat(salary).isEqualTo(2000);
	}

	@Test
	public void agg_withoutGroupBy_count() throws Exception {
		//arrange
		SourceLoader employee = loadEmployee();
		Aggregator avgSalary = Aggregator.builder()
				.name("aggerator")
				.input(DatasetRefs.fromTransform(employee))
				.aggregator(AggregateField.count("*", "count"))
				.output(DatasetRefs.datasetRef("Employee"))
				.build();

		PipelineJobRequest pipeline = PipelineJobRequest.builder().name("agg_count").step(
				PipelineStep.builder().name("step1")
						.transform(employee)
						.transform(avgSalary)
						.build())
				.build();

		//act
		getJobOperator().runJob(pipeline);

		//assert
		long count = driverContext.getSparkSession().sql("select * from Employee").first().getAs("count");
		assertThat(count).isEqualTo(5);
	}
}