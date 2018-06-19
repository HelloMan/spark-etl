package etl.spark.pipeline.dq;

import etl.api.job.pipeline.PipelineJobRequest;
import etl.api.job.pipeline.PipelineStep;
import etl.api.pipeline.DatasetRef;
import etl.api.pipeline.DatasetRefs;
import etl.api.pipeline.FieldType;
import etl.api.pipeline.Join;
import etl.api.pipeline.JoinField;
import etl.api.pipeline.JoinOn;
import etl.api.pipeline.JoinType;
import etl.api.pipeline.Map;
import etl.api.pipeline.MapField;
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

/**
 * Result should be:
 * +----------+--------------------+-------------+------------+------------+---------+------------+-----------+--------------------+--------+
 * |ENTITYCODE|          REPORTDATE|TABLERECORDID|ASSETPRODUCT| ASSETSOURCE|TABLECODE|VALIDATIONID|  FIELDNAME|           EXCEPTION|SEVERITY|
 * +----------+--------------------+-------------+------------+------------+---------+------------+-----------+--------------------+--------+
 * |      CONA|2016-05-31 00:00:...|            3|    Capacity|Bank of Japa|       IA|  RI_RSPR_IA|AssetSource|Sub-product "Bank...|       2|
 * |      CONA|2016-05-31 00:00:...|           21|    Capacity|        null|       IA|  RI_RSPR_IA|AssetSource|Product "Capacity...|       2|
 * |      CONA|2016-05-31 00:00:...|           22|    Capacity|        test|       IA|  RI_RSPR_IA|AssetSource|Sub-product "test...|       2|
 * +----------+--------------------+-------------+------------+------------+---------+------------+-----------+--------------------+--------+
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = TestPipelineApplication.class)
public class Fry2052A extends AbstractTransformTest {



	@Test
	public void run() throws Exception {

		PipelineJobRequest job = PipelineJobRequest.builder()
				.name("Fr2052a Example - RI_RSPR_IA")
				.step(createStep())
				.build();

		getJobOperator().runJob(job);
		Dataset<Row> dataset = driverContext.getSparkSession().sql("select * from RI_RSPR_IA");
		Row[] result = (Row[]) dataset.collect();
		assertThat(result.length).isEqualTo(3);
		assertThat(result[0].getAs("EXCEPTION").toString())
				.isEqualTo("Sub-product [Bank of Japa] is not acceptable for product [Capacity]");

	}

	/**
	 * Translated from FR2052a SQL in AR:
	 *
	 * SELECT
	 * :EntityCode	AS EntityCode,
	 * :ReportDate	AS ReportDate,
	 * 'IA'			AS TableCode,
	 * 'RI_RSPR_IA'	AS ValidationId,
	 * RecordId		AS TableRecordId,
	 * 'AssetSource'	AS FieldName,
	 * CASE
	 * WHEN AssetSource IS NULL THEN 'Product "' ||  AssetProduct || '" require the reporting of a Sub-Product. Sub-Product is missing'
	 * ELSE 'Sub-product "' || AssetSource  || '" is not acceptable for product "' ||  AssetProduct ||'"'
	 * END				AS Exception,
	 * 2				AS Severity
	 * FROM ${SCHEMA_NAME}.AssetInflow a
	 * JOIN  ${SCHEMA_NAME}.StandardProduct b ON a.AssetProduct = b.Description and b.PID IN ('IA2', 'IA3', 'IA4')
	 * LEFT OUTER JOIN ${SCHEMA_NAME}.vStandardProductRequisite c
	 * ON a.AssetProduct = c.Product AND a.AssetSource = c.Requisite AND c.TableId = 'IA' AND c.CategoryId = 1
	 * WHERE c.Product IS NULL
	 * AND EntityCode = :EntityCode
	 * AND ReferenceDate = :ReportDate
	 */
	private PipelineStep createStep() {
		PipelineStep.PipelineStepBuilder stepBuilder = PipelineStep.builder().name("RI_RSPR_IA");

		SourceLoader assetInFlow = SourceLoader.builder()
				.name("AssetInFlow")
				.source(TransformUtil.getCsvSource("data/fr2052a/AssetInFlow.csv"))
				.build();
		SourceLoader standardProduct = SourceLoader.builder()
				.name("StandardProduct")
				.source(TransformUtil.getCsvSource("data/fr2052a/StandardProduct.csv"))
				.build();
		SourceLoader vStandardProductRequisite = SourceLoader.builder()
				.name("vStandardProductRequisite")
				.source(TransformUtil.getCsvSource("data/fr2052a/vStandardProductRequisite.csv"))
				.build();

		DatasetRef ds1 = DatasetRefs.fromTransform(assetInFlow);
		DatasetRef ds2 = DatasetRefs.fromTransform(standardProduct, "PID in ('IA2', 'IA3', 'IA4')");
		DatasetRef ds3 = DatasetRefs.fromTransform(vStandardProductRequisite, "TABLEID='IA' and CATEGORYID=1");

		Join join1 = Join.join(ds1, ds2, JoinType.INNER)
				.name("join1")
				.on(JoinOn.on("ASSETPRODUCT", "DESCRIPTION"))
				.field(JoinField.left("RECORDID"))
				.field(JoinField.left("ENTITYCODE"))
				.field(JoinField.left("REFERENCEDATE"))
				.field(JoinField.left("ASSETPRODUCT"))
				.field(JoinField.left("ASSETSOURCE"))
				.build();

		Join join2 = Join.join(DatasetRefs.fromTransform(join1), ds3, JoinType.LEFT)
				.name("join2")
				.on(JoinOn.on("ASSETPRODUCT", "PRODUCT"))
				.on(JoinOn.on("ASSETSOURCE", "REQUISITE"))
				.field(JoinField.left("ENTITYCODE"))
				.field(JoinField.left("REFERENCEDATE", "REPORTDATE"))
				.field(JoinField.left("RECORDID", "TABLERECORDID"))
				.field(JoinField.left("ASSETPRODUCT"))
				.field(JoinField.left("ASSETSOURCE"))
				.build();

		Map map = Map.builder()
				.input(DatasetRefs.fromTransform(join2, "PRODUCT is null"))
				.output(DatasetRef.builder().name("RI_RSPR_IA").build())
				.name("map")
				.field(MapField.builder().name("'IA'").as("TABLECODE").fieldType(FieldType.STRING).build())
				.field(MapField.builder().name("'RI_RSPR_IA'").as("VALIDATIONID").fieldType(FieldType.STRING).build())
				.field(MapField.builder().name("'AssetSource'").as("FIELDNAME").fieldType(FieldType.STRING).build())
				.field(MapField.builder()
						.name("if (empty(ASSETSOURCE)) {" +
								"`Product [${ASSETPRODUCT}] require the reporting of a Sub-Product. Sub-Product is missing`" +
								"} else {" +
								"`Sub-product [${ASSETSOURCE}] is not acceptable for product [${ASSETPRODUCT}]`" +
								"}")
						.as("EXCEPTION")
						.fieldType(FieldType.STRING)
						.build())
				.field(MapField.builder().name("2").as("SEVERITY").fieldType(FieldType.INT).build())
				.build();

		stepBuilder
				.transform(assetInFlow)
				.transform(standardProduct)
				.transform(vStandardProductRequisite);

		return stepBuilder
				.transform(join1)
				.transform(join2)
				.transform(map)
				.build();
	}
}