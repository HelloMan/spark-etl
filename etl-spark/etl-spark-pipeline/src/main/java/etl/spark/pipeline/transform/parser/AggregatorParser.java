package etl.spark.pipeline.transform.parser;

import etl.api.pipeline.AggregateField;
import etl.api.pipeline.Aggregator;
import etl.api.pipeline.DatasetRefs;
import etl.spark.pipeline.core.StepExecution;
import etl.spark.pipeline.core.TransformDatasetService;
import etl.spark.pipeline.core.PipelineException;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.util.List;
import java.util.stream.Collectors;

@SuppressWarnings("squid:UndocumentedApi")
public class AggregatorParser extends AbstractTransformParser<Aggregator> {


	public AggregatorParser(TransformDatasetService transformDatasetService, StepExecution stepExecution) {
		super(transformDatasetService,stepExecution);
	}

	@Override
	public void parse(Aggregator transform) {
		final Dataset<Row> inputDataFrame = transformDatasetService.getDataset(stepExecution,transform.getInput())
				.orElseThrow(() -> new PipelineException(String.format("Dataset can not be found for transform's input %s", transform.getInput())));
		Dataset<Row> dataFrame = inputDataFrame;

		List<Column> aggregateColumns = getAggregateColumns(transform, inputDataFrame);

		Column[] groupColumns = getGroupColumns(transform, inputDataFrame);
		
		if (transform.hasGroupBys()) {
			if (aggregateColumns.size() == 1) {
				dataFrame = dataFrame.groupBy(groupColumns).agg(aggregateColumns.get(0));
			} else {
				Column tailAggregateColumn = aggregateColumns.stream().reduce((a, b) -> b).orElseThrow(() -> new PipelineException("No aggregateColumn found"));
				dataFrame = dataFrame.groupBy(groupColumns).agg(aggregateColumns.get(0), tailAggregateColumn);
			}
		} else {
			if (aggregateColumns.size() == 1) {
				dataFrame = dataFrame.agg(aggregateColumns.get(0));
			} else {
				Column tailAggregateColumn = aggregateColumns.stream().reduce((a, b) -> b).orElseThrow(() -> new PipelineException("No aggregateColumn found"));
				dataFrame = dataFrame.agg(aggregateColumns.get(0), tailAggregateColumn);
			}
		}

		transformDatasetService.persistDataset(stepExecution,DatasetRefs.fromTransform(transform), dataFrame);
	}

	private Column[] getGroupColumns(Aggregator transform, Dataset<Row> inputDataFrame) {
		return transform.getGroupBys().stream().map(inputDataFrame::col).toArray(Column[]::new);
	}

	private List<Column> getAggregateColumns(Aggregator transform, Dataset<Row> inputDataFrame) {
		return transform.getAggregators().stream().map(input -> getColumnByAggregateProjection(inputDataFrame, input)).collect(Collectors.toList());
	}

	private Column getColumnByAggregateProjection(Dataset<Row> inputDataFrame, AggregateField input) {
		switch (input.getType()) {
			case AVG:
				return functions.avg(inputDataFrame.col(input.getName())).as(input.getAlias());
			case MAX:
				return functions.max(inputDataFrame.col(input.getName())).as(input.getAlias());
			case MIN:
				return functions.min(inputDataFrame.col(input.getName())).as(input.getAlias());
			case SUM:
				return functions.sum(inputDataFrame.col(input.getName())).as(input.getAlias());
			case COUNT:
				return functions.count(inputDataFrame.col(input.getName())).as(input.getAlias());
			default:
				throw new IllegalArgumentException(String.format("Aggregate type %s is not support", input.getType()));
		}

	}


}
