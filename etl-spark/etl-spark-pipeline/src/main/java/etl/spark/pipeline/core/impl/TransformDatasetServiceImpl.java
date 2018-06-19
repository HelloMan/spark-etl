package etl.spark.pipeline.core.impl;

import com.google.common.base.Preconditions;
import etl.api.dataset.DatasetMetadata;
import etl.api.job.pipeline.PipelineStepHelper;
import etl.api.parameter.Parameter;
import etl.api.parameter.Parameters;
import etl.api.pipeline.DatasetRef;
import etl.api.pipeline.DatasetRefParam;
import etl.api.pipeline.DatasetRefs;
import etl.api.pipeline.Field;
import etl.client.DatasetClient;
import etl.spark.core.DataFrameIOService;
import etl.spark.core.DriverContext;
import etl.spark.pipeline.core.PipelineException;
import etl.spark.pipeline.core.StepExecution;
import etl.spark.pipeline.core.TransformDatasetService;
import javaslang.Tuple2;
import lombok.extern.slf4j.Slf4j;
import one.util.streamex.StreamEx;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.stream.Collectors;

@Slf4j
@Component
public class TransformDatasetServiceImpl implements TransformDatasetService {

	@Autowired
	private DriverContext driverContext;

	@Autowired
	private DatasetClient datasetClient;

	@Autowired
	private transient DataFrameIOService dataFrameIOService;

	@Override
	public void persistDataset(StepExecution stepExecution, DatasetRef output, org.apache.spark.sql.Dataset<Row> dataFrame) {
		stepExecution.getJobExecutionContext().addDataset(output, dataFrame);
		final Dataset<Row> newDataFrame = includeFields(output.getIncludeFields(), output.getExcludeFields(), dataFrame);
		if (driverContext.isDebugMode()) {
			newDataFrame.createOrReplaceTempView(output.getName());
		} else {
			if (!output.isImmediate()) {
				DatasetMetadata datasetMetadata = output.toDatasetMetadata(stepExecution.getJobExecution().
						getPipelineJobRequest().getParameters());
				dataFrameIOService.writeDataFrame(newDataFrame, datasetMetadata, null);
			}
		}
		printDataset(output, newDataFrame);
	}



	@Override
	public Optional<Dataset<Row>> getDataset(StepExecution stepExecution, DatasetRef input) {
		Dataset<Row> result = stepExecution.getJobExecutionContext().getDataset(input);
		if (result == null) {
			final DatasetRef inputWithoutFilter = DatasetRefs.datasetRef(input.getName(), input.getParameters());
			result = stepExecution.getJobExecutionContext().getDataset(inputWithoutFilter);
			if (result == null) {
				etl.api.dataset.Dataset dataset = getRegisteredDataset(stepExecution, input);
				result = dataFrameIOService.readDataFrame(dataset);
				stepExecution.getJobExecutionContext().addDataset(inputWithoutFilter, result);
			}
			if (result != null && StringUtils.isNotEmpty(input.getFilter())) {
				result = result.filter(input.getFilter());
				stepExecution.getJobExecutionContext().addDataset(input, result);
			}
		}
		return Optional.ofNullable(result);
	}


	private Dataset<Row> includeFields(List<Field> includes, List<Field> excludes,
									   org.apache.spark.sql.Dataset<Row> dataset) {
		Preconditions.checkArgument(!(CollectionUtils.isNotEmpty(includes) && CollectionUtils.isNotEmpty(excludes)),
				"Can't assign select fields and deselect fields at the same time");
		if (CollectionUtils.isNotEmpty(includes)) {
			return includes(includes, dataset);
		}
		if (CollectionUtils.isNotEmpty(excludes)) {
			return excludes(excludes, dataset);
		}
		return dataset;
	}

	private Dataset<Row> includes(List<Field> includes, org.apache.spark.sql.Dataset<Row> dataset) {
		Preconditions.checkArgument(includes.stream().noneMatch(Field::isAll),
				"Can't assign \"*\" as include field");
		return dataset.select(includes.stream()
				.map(field -> dataset.col(field.getName()).as(field.getAs()))
				.toArray(Column[]::new));
	}

	private Dataset<Row> excludes(List<Field> excludes, org.apache.spark.sql.Dataset<Row> dataset) {
		Preconditions.checkArgument(excludes.stream().noneMatch(Field::isAll),
				"Can't assign \"*\" as exclude field");
		List<String> allFields = Arrays.asList(dataset.schema().fieldNames());
		Collection<String> selects = CollectionUtils.subtract(allFields, excludes.stream()
				.map(Field::getName)
				.map(Field::of)
				.collect(Collectors.toList()));

		List<Field> selectsAs = selects.stream().map(Field::of).collect(Collectors.toList());
		return includes(selectsAs, dataset);
	}


	private void printDataset(DatasetRef output, org.apache.spark.sql.Dataset<Row> dataFrame) {
		if (driverContext.isDebugMode()) {
			log.info("Transform output : {}", output);
			dataFrame.show();
		}
	}

	private etl.api.dataset.Dataset getRegisteredDataset(StepExecution stepExecution, DatasetRef input) {
		PipelineStepHelper pipelineStepHelper = new PipelineStepHelper(stepExecution.getStep());
		if (pipelineStepHelper.isImmediateDatasetRef(input)) {
			etl.api.dataset.Dataset dataset = new etl.api.dataset.Dataset();
			dataset.setJobExecutionId(driverContext.getJobExecutionId());
			dataset.setName(input.getName());
			return dataset;
		} else {
			try {
				Parameters jobParams = stepExecution.getJobExecution().getPipelineJobRequest().getParameters();
				Parameters datasetParams = buildDatasetParams(jobParams, input.getParameters());
				etl.api.dataset.Dataset dataset = datasetClient.getLastVersionOfDataset(input.getName(), datasetParams).execute().body();
				return dataset;
			} catch (IOException e) {
				throw new PipelineException(e);
			}
		}
	}

	private Parameters buildDatasetParams(Parameters jobParams, SortedSet<DatasetRefParam> datasetParams) {
		Map<String, Parameter> map = StreamEx.of(datasetParams)
				.map(refParam -> {
					Object value = jobParams.getObject(refParam.getTransformParam());
					return new Tuple2<>(refParam.getDatasetParam(), value);
				})
				.mapToEntry(t -> Parameter.create(t._1, t._2))
				.mapKeys(t -> t._1)
				.toMap();
		return new Parameters(map);
	}

}
