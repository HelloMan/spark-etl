package etl.spark.pipeline.transform.parser;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import etl.api.pipeline.DatasetRefs;
import etl.api.pipeline.Join;
import etl.api.pipeline.JoinField;
import etl.api.pipeline.JoinOn;
import etl.spark.pipeline.core.PipelineException;
import etl.spark.pipeline.core.StepExecution;
import etl.spark.pipeline.core.TransformDatasetService;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class JoinParser extends AbstractTransformParser<Join> {


	public JoinParser(TransformDatasetService transformDatasetService,StepExecution stepExecution) {
		super(transformDatasetService,stepExecution);
	}

	@Override
	public void parse(final Join operator) {
		Dataset<Row> leftDataFrame = transformDatasetService.getDataset(stepExecution,operator.getLeft())
				.orElseThrow(() -> new PipelineException(String.format("Dataset can not be found for transform's input %s", operator.getLeft())));
		Dataset<Row> rightDataFrame = transformDatasetService.getDataset(stepExecution,operator.getRight())
				.orElseThrow(() -> new PipelineException(String.format("Dataset can not be found for transform's input %s", operator.getRight())));

		Set<String> commonFields = getCommonFieldsByNameAndType(leftDataFrame, rightDataFrame);
		if (commonFields.size() > 0) {
			leftDataFrame = replaceDataSet(leftDataFrame, commonFields, true);
			rightDataFrame = replaceDataSet(rightDataFrame, commonFields, false);
		}

		Column joinColumns = getJoinColumns(operator, leftDataFrame, rightDataFrame);

		final Column[] projectionColumns = getProjectionColumns(commonFields, operator, leftDataFrame, rightDataFrame);

		Dataset<Row> dataset = leftDataFrame.join(rightDataFrame, joinColumns, operator.getJoinType().toString()).select(projectionColumns);

		transformDatasetService.persistDataset(stepExecution,DatasetRefs.fromTransform(operator), dataset);
	}

	private Dataset<Row> replaceDataSet(final Dataset<Row> dataset, final Set<String> fieldsNeedToBeReplace, final boolean left) {
		List<Column> privateColumns = Arrays.stream(dataset.schema().fields())
				.filter(f -> !fieldsNeedToBeReplace.contains(f.name()))
				.map(f -> dataset.col(f.name()))
				.collect(Collectors.toList());

		List<Column> selectColumns = Lists.newArrayList(privateColumns);

		Dataset<Row> replaceDataSet = dataset;
		for (String needToBeReplace : fieldsNeedToBeReplace) {
			String replaceField = getReplaceFieldName(left, needToBeReplace);
			replaceDataSet = replaceDataSet.withColumn(replaceField, dataset.col(needToBeReplace));
			selectColumns.add(replaceDataSet.col(replaceField));
		}

		Column[] resultColumns = FluentIterable.from(selectColumns).toArray(Column.class);
		return replaceDataSet.select(resultColumns);
	}

	private String getReplaceFieldName(boolean left, String fieldName) {
		return fieldName + "_" + (left ? "l" : "r");
	}

	private Set<String> getCommonFieldsByNameAndType(Dataset<Row> left, Dataset<Row> right) {
		StructField[] fields = left.schema().fields();
		Map<String, StructField> fieldMap = Arrays.stream(fields)
				.collect(Collectors.toMap(StructField::name, Function.identity()));
		return Arrays.stream(right.schema().fields())
				.filter(f -> isSameDataType(f, fieldMap))
				.map(f -> f.name())
				.collect(Collectors.toSet());
	}

	private boolean isSameDataType(StructField structField, Map<String, StructField> fieldMap) {
		StructField matchField = fieldMap.get(structField.name());
		return matchField != null && matchField.dataType().sameType(structField.dataType());
	}

	private Column[] getProjectionColumns(final Set<String> commonFields, Join transform, final Dataset<Row> leftDataFrame,
										  final Dataset<Row> rightDataFrame) {
		List<Column> allColumns = new ArrayList<>();

		for (JoinField joinProjection : transform.getFields()) {
			if (joinProjection.isAll()) {
				buildStarColumns(commonFields, joinProjection.isFromLeftSide() ? leftDataFrame : rightDataFrame,
						allColumns, joinProjection.isFromLeftSide());
			} else {
				allColumns.add(getPrivateProjectColumn(commonFields, joinProjection, leftDataFrame, rightDataFrame));
			}
		}
		return allColumns.toArray(new Column[allColumns.size()]);
	}

	private Column getPrivateProjectColumn(Set<String> commonFields, JoinField field, Dataset<Row> leftDataFrame, Dataset<Row> rightDataFrame) {
		String inputField = field.getName();

		String originalField = null;
		Dataset<Row> inputDataFrame = field.isFromLeftSide() ? leftDataFrame : rightDataFrame;
		for (String comonField : commonFields) {
			if (inputField.contains(comonField)) {
				originalField = comonField;
				inputField = inputField.replace(comonField, getReplaceFieldName(field.isFromLeftSide(), comonField));
				break;
			}
		}

		if (StringUtils.isNotEmpty(field.getAs())) {
			return inputDataFrame.col(inputField).as(field.getAs());
		} else {
			if (originalField != null) {
				return inputDataFrame.col(inputField).as(originalField);
			} else {
				return inputDataFrame.col(inputField);
			}
		}
	}

	private void buildStarColumns(Set<String> commonFields, Dataset<Row> leftDataFrame, List<Column> allColumns, boolean leftSide) {
		for (StructField structField : leftDataFrame.schema().fields()) {
			String fieldName = structField.name();
			boolean isCommonField = false;
			for (String comonField : commonFields) {
				if (getReplaceFieldName(leftSide, comonField).equals(fieldName)) {
					allColumns.add(leftDataFrame.col(fieldName).as(comonField));
					isCommonField = true;
					break;
				}
			}
			if (!isCommonField) {
				allColumns.add(leftDataFrame.col(fieldName));
			}
		}
	}

	private Column getJoinColumns(Join transform, Dataset<Row> leftDataFrame, Dataset<Row> rightDataFrame) {
		Column joinColumns = null;
		for (JoinOn joinOn : transform.getOns()) {

			String leftColumn = joinOn.getLeft();
			String rightColumn = joinOn.getRight();
			if (leftColumn.equals(rightColumn)) {
				leftColumn = getReplaceFieldName(true, leftColumn);
				rightColumn = getReplaceFieldName(false, rightColumn);
			}
			Column joinColumn = leftDataFrame.col(leftColumn).equalTo(rightDataFrame.col(rightColumn));

			if (joinColumns != null) {
				joinColumns = joinColumns.and(joinColumn);
			} else {
				joinColumns = joinColumn;
			}
		}
		return joinColumns;
	}
	
}
