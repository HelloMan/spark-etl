package etl.spark.pipeline.transform.parser;

import etl.api.job.pipeline.PipelineJobRequest;
import etl.api.job.pipeline.PipelineStep;
import etl.api.pipeline.FieldType;
import etl.api.pipeline.Map;
import etl.api.pipeline.MapField;
import etl.api.pipeline.SourceLoader;
import etl.api.pipeline.DatasetRefs;
import etl.api.pipeline.DatasetRef;
import etl.spark.TestPipelineApplication;
import etl.spark.pipeline.transform.TransformUtil;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static org.assertj.core.api.Assertions.assertThat;
@RunWith(SpringRunner.class)
@SpringBootTest(classes = TestPipelineApplication.class)
public class MapParserTest extends AbstractTransformTest {

	
	/**
	 * Put these 2 lines in MapTransformParser.evaluate() method (remove existed lines) to run this example:
	 * javaslang.collection.List<String> split = javaslang.collection.List.of(expression.split("\\+"));
	 * return split.map(f -> Integer.valueOf(rowMap.get(f).toString())).sum();
	 */
	@Test
	public void simple_mapTransform() throws Exception {
		//arrange
		PipelineJobRequest job = PipelineJobRequest.builder().name("simple_mapTransform").step(createSimpleOperatorStep()).build();
		getJobOperator().runJob(job);

		//assert
		SparkSession spark = driverContext.getSparkSession();
		Integer sum1 = spark.sql("select * from res1").first().getAs("sum1");
		Integer sum2 = spark.sql("select * from res2").first().getAs("sum2");
		assertThat(sum1).isEqualTo(3);
		assertThat(sum2).isEqualTo(7);

	}
	
	private PipelineStep createSimpleOperatorStep() {
		PipelineStep.PipelineStepBuilder stepBuilder = PipelineStep.builder().name("ProcessingInboundAssets");
		
		SourceLoader csvLoader1 = SourceLoader.builder().source(TransformUtil.getCsvSource("data/nums1.csv")).name("nums1").build();
		SourceLoader csvLoader2 = SourceLoader.builder().source(TransformUtil.getCsvSource("data/nums2.csv")).name("nums2").build();

		DatasetRef out1 = DatasetRefs.datasetRef("res1");
		Map transform1 = Map.builder().input(DatasetRefs.fromTransform(csvLoader1)).output(out1).name("mapTransform")
				.field(MapField.builder().name("n1+n2").as("sum1").fieldType(FieldType.of(FieldType.Type.INT)).build())
				.build();
		
		DatasetRef out2 = DatasetRefs.datasetRef("res2");
		Map transform2 = Map.builder().input(DatasetRefs.fromTransform(csvLoader2)).output(out2).name("map2")
				.field(MapField.builder().name("m1+m2").as("sum2").fieldType(FieldType.of(FieldType.Type.INT)).build())
				.build();
		
		stepBuilder.transform(csvLoader1);
		stepBuilder.transform(csvLoader2);
		stepBuilder.transform(transform1);
		stepBuilder.transform(transform2);
		return stepBuilder.build();
	}
	

}