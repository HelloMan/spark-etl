package etl.spark.pipeline.transform;

import etl.api.pipeline.Distinct;
import etl.api.pipeline.Join;
import etl.api.pipeline.JoinField;
import etl.api.pipeline.JoinOn;
import etl.api.pipeline.JoinType;
import etl.api.pipeline.Map;
import etl.api.pipeline.PartitionField;
import etl.api.pipeline.PartitionMap;
import etl.api.job.pipeline.PipelineJobRequest;
import etl.api.job.pipeline.PipelineStep;
import etl.api.pipeline.SourceLoader;
import etl.api.pipeline.DatasetRefs;
import etl.api.pipeline.DatasetRef;
import etl.api.datasource.CsvDataSource;

public class TransformUtil {

	public static CsvDataSource getCsvSource(String fileName) {
		return CsvDataSource.builder()
				.filePath("src/test/resources/" + fileName)
				.header(true)
				.build();
	}



	public static PipelineJobRequest buildPipeline() {
		return PipelineJobRequest.builder().name("pipelineExample").step(createStep().build()).build();
	}

	private static PipelineStep.PipelineStepBuilder createStep() {
		PipelineStep.PipelineStepBuilder stepBuilder = PipelineStep.builder().name("InboundAssets");
		SourceLoader sourceLoader = SourceLoader.builder()
				.name("src")
				.source(getCsvSource("data/InboundAssets.csv"))
				.build();

		// "select a.*, count(*) over(partition by SubmissionType, Country) as countRow from A a",
		PartitionField partitionField = PartitionField.count("*").as("countRow")
				.partitionBy("SubmissionType")
				.partitionBy("Country").build();
		PartitionMap partitionMap1 = PartitionMap.builder()
				.name("partitionMap1")
				.input(DatasetRefs.fromTransform(sourceLoader, "AbsWidening != 'na'"))
				.field(partitionField).build();

		// "select * from B where countRow >= 4 and Bps in (50, 100, 500, 1000)"
		// "select distinct AbsWidening, Bps, SubmissionType, Country from D",
		Distinct distinctTransform = Distinct.builder()
				.input(DatasetRefs.fromTransform(partitionMap1, "countRow >= 4 and Bps in (50, 100, 500, 1000)"))
				.name("distrinct")
				.field("AbsWidening")
				.field("Bps")
				.field("SubmissionType")
				.field("Country")
				.build();

		// "select e.*, count(*) over(partition by SubmissionType, Country) countRow from E e"
		PartitionField partitionField2 = PartitionField.count("*")
				.as("countRow")
				.partitionBy("SubmissionType")
				.partitionBy("Country")
				.build();
		PartitionMap partitionMap2 = PartitionMap.builder()
				.name("partitionMap2")
				.input(DatasetRefs.fromTransform(distinctTransform))
				.field(partitionField2)
				.build();

		// "select a.*, g.AbsWidening AbsWidening_2 from InboundAssets a left join G g on (a.AbsWidening=g.AbsWidening and a.SubmissionType=g.SubmissionType and a.Country=g.Country)"
		Join joinTransform = Join.join(DatasetRefs.fromTransform(sourceLoader), DatasetRefs.fromTransform(partitionMap2, "countRow >= 4"), JoinType.LEFT)
				.name("join")
				.on(JoinOn.on("AbsWidening", "AbsWidening"))
				.on(JoinOn.on("SubmissionType", "SubmissionType"))
				.field(JoinField.left("ID"))
				.field(JoinField.left("AbsWidening"))
				.field(JoinField.left("Bps"))
				.field(JoinField.left("SubmissionType"))
				.field(JoinField.left("Country"))
				.field(JoinField.right("countRow"))
				.build();

		// "select ID, AbsWidening, SubmissionType, Country from H where countRow is null"
		Map mapTransform = Map.builder()
				.name("result")
				.input(DatasetRefs.fromTransform(joinTransform, "countRow is null"))
				.output(DatasetRef.builder()
						.name("outputResult")
						.exclude("countRow")
						.build())
				.build();

		stepBuilder.transform(sourceLoader);
		stepBuilder.transform(partitionMap1);
		stepBuilder.transform(distinctTransform);
		stepBuilder.transform(partitionMap2);
		stepBuilder.transform(joinTransform);
		stepBuilder.transform(mapTransform);

		return stepBuilder;
	}

	public static PipelineJobRequest buildPipelineWithCsvOutput() {
		PipelineStep.PipelineStepBuilder stepBuilder = createStep();
		Map dummyTransform = Map.builder()
				.name("result")
				.input(DatasetRefs.datasetRef("dumytransform"))
				.output(DatasetRef.builder()
						.name("outputResult")
						.exclude("countRow")
						.build())
				.build();
		stepBuilder.transform(dummyTransform);
		return PipelineJobRequest.builder().name("pipelineExample").step(stepBuilder.build()).build();
	}
	/**
	 * test expected exception
	 * @return PipelineJobRequest
	 */
	public static PipelineJobRequest buildPipelineWithEmptyTransform() {

		return PipelineJobRequest.builder().name("pipelineExample").step(createStepWithEmptyTransform()).build();

	}

	private static PipelineStep createStepWithEmptyTransform() {
		PipelineStep.PipelineStepBuilder stepBuilder = PipelineStep.builder().name("InboundAssets");
		return stepBuilder.build();
	}


}
