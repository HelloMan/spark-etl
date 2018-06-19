package etl.spark.pipeline.dq;

import etl.api.job.pipeline.PipelineJobRequest;
import etl.api.job.pipeline.PipelineStep;
import etl.api.pipeline.DatasetRef;
import etl.api.pipeline.DatasetRefs;
import etl.api.pipeline.Map;
import etl.api.pipeline.SourceLoader;
import etl.spark.TestPipelineApplication;
import etl.spark.pipeline.transform.TransformUtil;
import etl.spark.pipeline.transform.parser.AbstractTransformTest;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static org.assertj.core.api.Assertions.assertThat;
@RunWith(SpringRunner.class)
@SpringBootTest(classes = TestPipelineApplication.class)
public class Fry14Q extends AbstractTransformTest {

	@Test
	public void run() throws Exception {
		PipelineJobRequest job = PipelineJobRequest.builder()
				.name("FRY14Q Example - Posrisk_RF")
				.step(createStep())
				.build();

		getJobOperator().runJob(job);
		Dataset<Row> dataset = driverContext.getSparkSession().sql("select * from Posrisk_RF");
		Row[] result = (Row[]) dataset.collect();
		assertThat(result.length).isEqualTo(5);
		assertThat(result[0].getAs("BASEL_BUSLINE1").toString()).isEqualTo("BL18");

	}

	private PipelineStep createStep() {
		PipelineStep.PipelineStepBuilder stepBuilder = PipelineStep.builder().name("BASEL_BUSLINE1_CHECK");

		SourceLoader posriskrf = SourceLoader.builder()
				.name("AssetInFlow")
				.source(TransformUtil.getCsvSource("data/fry14q/Posrisk_RF.csv"))
				.build();

		DatasetRef ds1;
		String filter = "BASEL_BUSLINE1 not in ('BL1', 'BL2', 'BL3', 'BL4', 'BL5', 'BL6', 'BL7', 'BL8', 'BL9')";


		ds1 = DatasetRefs.fromTransform(posriskrf, filter);


		Map map = Map.builder()
				.input(ds1)
				.output(DatasetRef.builder()
						.name("Posrisk_RF")
						.build())
				.name("map")
				.build();


		stepBuilder.transform(posriskrf);


		return stepBuilder
				.transform(map)
				.build();
	}
}