package etl.spark.pipeline.core.impl;

import com.google.common.collect.ImmutableList;
import etl.api.job.pipeline.PipelineJobRequest;
import etl.api.job.pipeline.PipelineStep;
import etl.api.parameter.StringParameter;
import etl.api.pipeline.DatasetRef;
import etl.api.pipeline.DatasetRefParam;
import etl.spark.TestPipelineApplication;
import etl.spark.pipeline.core.JobExecution;
import etl.spark.pipeline.core.StepExecution;
import etl.spark.pipeline.core.TransformDatasetService;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = TestPipelineApplication.class)
public class TransformDatasetServiceImplTest {
	@Autowired
	private SparkSession sparkSession;

	private PipelineJobRequest pipelineJobRequest;

	private JobExecution jobExecution;
	private StepExecution stepExecution;
	@Autowired
	private TransformDatasetService transformDatasetService;

	@Before
	public void setUp() {
		PipelineStep pipelineStep = PipelineStep.builder()
				.name("step")
				.build();
		pipelineJobRequest = PipelineJobRequest.builder()
				.name("pipeline")
				.step(pipelineStep)
				.jobParameter(new StringParameter("ENTITYCODE", "0001"))
				.build();
		jobExecution = new JobExecution(1l, pipelineJobRequest);
		stepExecution = new StepExecution(jobExecution, pipelineStep);
	}

	@Test
	public void testPersistDataset() throws Exception {
		DatasetRef datasetRef = DatasetRef.builder()
				.name("Foo2")
				.parameter(new DatasetRefParam("GroupName", "ENTITYCODE"))
				.build();

		StructField structField1 = new StructField("id1", DataTypes.IntegerType, true, Metadata.empty());
		StructField structField2 = new StructField("id2", DataTypes.IntegerType, true, Metadata.empty());
		StructField[] fields = {structField1, structField2};
		StructType structType = new StructType(fields);

		Row row = RowFactory.create(1, 2);
		Dataset<Row> dataFrame = sparkSession.createDataFrame(ImmutableList.of(row), structType);
		transformDatasetService.persistDataset(stepExecution, datasetRef, dataFrame);
		assertThat(sparkSession.sql("select * from Foo2").count()).isEqualTo(1);
	}

	@Test
	public void testGetDataset() throws Exception {
		DatasetRef datasetRef = DatasetRef.builder()
				.name("MockDataset")
				.parameter(new DatasetRefParam("GroupName", "ENTITYCODE"))
				.build();

		StructField f1 = new StructField("RUN_KEY", DataTypes.LongType, true, Metadata.empty());
		StructField f2 = new StructField("id1", DataTypes.IntegerType, true, Metadata.empty());
		StructField f3 = new StructField("id2", DataTypes.IntegerType, true, Metadata.empty());
		StructField[] fields = {f1, f2, f3};
		StructType structType = new StructType(fields);

		Row row1 = RowFactory.create(1L, 1, 2);
		Row row2 = RowFactory.create(1L, 3, 4);
		Row row3 = RowFactory.create(2L, 5, 6);
		Dataset<Row> dataFrame = sparkSession.createDataFrame(ImmutableList.of(row1, row2, row3), structType);
		dataFrame.createOrReplaceTempView("MockDataset");

		Optional<Dataset<Row>> datasetOptional = transformDatasetService.getDataset(stepExecution, datasetRef);
		assertThat(datasetOptional.isPresent()).isTrue();
		assertThat(datasetOptional.get().count()).isEqualTo(2);
	}
}