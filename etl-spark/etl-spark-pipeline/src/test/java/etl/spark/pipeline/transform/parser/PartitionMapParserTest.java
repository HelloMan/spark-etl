package etl.spark.pipeline.transform.parser;

import etl.api.pipeline.PartitionField;
import etl.api.pipeline.PartitionMap;
import etl.api.job.pipeline.PipelineJobRequest;
import etl.api.job.pipeline.PipelineStep;
import etl.api.pipeline.SourceLoader;
import etl.api.pipeline.DatasetRefs;
import etl.spark.TestPipelineApplication;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static org.assertj.core.api.Assertions.assertThat;
@RunWith(SpringRunner.class)
@SpringBootTest(classes = TestPipelineApplication.class)
public class PartitionMapParserTest extends AbstractTransformTest {

	private SourceLoader employee;

	@Before
	public void init() {
		employee = loadEmployee();
	}

	@Test
	public void partition_max() throws Exception {
		PartitionMap partitionMap = PartitionMap.builder()
				.name("partition_max")
				.input(DatasetRefs.fromTransform(employee))
				.field(PartitionField.max("salary").as("maxSalary")
						.partitionBy("deptId")
						.build())
				.output(DatasetRefs.datasetRef("MaxSalary"))
				.build();

		PipelineJobRequest pipeline = getPipeline(partitionMap);

		getJobOperator().runJob(pipeline);
		long sumOfMaxSalary = driverContext.getSparkSession().sql("select sum(maxSalary) as sum from MaxSalary where deptId=1").first().getAs("sum");

		assertThat(sumOfMaxSalary).isEqualTo(6000);
	}

	@Test
	public void partition_sum() throws Exception {
		PartitionMap partitionMap = PartitionMap.builder()
				.name("partition_sum")
				.input(DatasetRefs.fromTransform(employee))
				.field(PartitionField.sum("salary").as("sumSalary")
						.partitionBy("deptId")
						.build())
				.output(DatasetRefs.datasetRef("SumSalary"))
				.build();

		PipelineJobRequest pipeline = getPipeline(partitionMap);

		getJobOperator().runJob(pipeline);
		long sumOfSalary = driverContext.getSparkSession().sql("select sumSalary from SumSalary where deptId=1").first().getAs("sumSalary");
		assertThat(sumOfSalary).isEqualTo(5000);
	}

	@Test
	public void partition_avg() throws Exception {
		PartitionMap partitionMap = PartitionMap.builder()
				.name("partition_avg")
				.input(DatasetRefs.fromTransform(employee))
				.field(PartitionField.avg("salary").as("avgSalary")
						.partitionBy("deptId")
						.build())
				.output(DatasetRefs.datasetRef("AvgSalary"))
				.build();

		PipelineJobRequest pipeline = getPipeline(partitionMap);

		getJobOperator().runJob(pipeline);
		double avgOfSalary = driverContext.getSparkSession().sql("select avgSalary from AvgSalary where deptId=1").first().getAs("avgSalary");
		assertThat(avgOfSalary).isEqualTo(2500);
	}

	@Test
	public void partition_min() throws Exception {
		PartitionMap partitionMap = PartitionMap.builder()
				.name("partition_min")
				.input(DatasetRefs.fromTransform(employee))
				.field(PartitionField.min("salary").as("minSalary")
						.partitionBy("deptId")
						.build())
				.output(DatasetRefs.datasetRef("MinSalary"))
				.build();

		PipelineJobRequest pipeline = getPipeline(partitionMap);

		getJobOperator().runJob(pipeline);
		int minOfSalary = driverContext.getSparkSession().sql("select minSalary from MinSalary where deptId=1").first().getAs("minSalary");
		assertThat(minOfSalary).isEqualTo(2000);
	}

	@Test
	public void partition_first_last() throws Exception {
		PartitionMap partitionMap = PartitionMap.builder()
				.name("partition_first")
				.input(DatasetRefs.fromTransform(employee))
				.field(PartitionField.first("salary").as("firstSalary")
						.partitionBy("deptId")
						.build())
				.field(PartitionField.last("salary").as("lastSalary")
						.partitionBy("deptId")
						.build())
				.output(DatasetRefs.datasetRef("Employee"))
				.build();

		PipelineJobRequest pipeline = getPipeline(partitionMap);

		getJobOperator().runJob(pipeline);
		int firstSalary = driverContext.getSparkSession().sql("select firstSalary from Employee where deptId=1").first().getAs("firstSalary");
		int lastSalary = driverContext.getSparkSession().sql("select lastSalary from Employee where deptId=1").first().getAs("lastSalary");
		assertThat(firstSalary).isEqualTo(2000);
		assertThat(lastSalary).isEqualTo(3000);
	}

	private PipelineJobRequest getPipeline(PartitionMap partitionMap) {
		return PipelineJobRequest.builder().name("partitionTest").step(
					PipelineStep.builder().name("step")
							.transform(employee)
							.transform(partitionMap)
							.build())
					.build();
	}

}
