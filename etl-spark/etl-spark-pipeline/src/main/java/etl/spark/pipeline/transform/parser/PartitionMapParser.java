package etl.spark.pipeline.transform.parser;

import etl.api.pipeline.DatasetRefs;
import etl.api.pipeline.PartitionField;
import etl.api.pipeline.PartitionMap;
import etl.spark.pipeline.core.PipelineException;
import etl.spark.pipeline.core.StepExecution;
import etl.spark.pipeline.core.TransformDatasetService;
import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

public class PartitionMapParser extends AbstractTransformParser<PartitionMap> {


    public PartitionMapParser(TransformDatasetService transformDatasetService,StepExecution stepExecution) {
        super(transformDatasetService,stepExecution);
    }

    @Override
    public void parse(PartitionMap transform) {
        Dataset<Row> inputDataFrame = transformDatasetService.getDataset(stepExecution,transform.getInput())
                .orElseThrow(() -> new PipelineException(String.format("Dataset can not be found for transform's input %s", transform.getInput())));
        Map<String, Column> windowsFields = new HashMap<>();
        for (PartitionField partitionField : transform.getFields()) {
            Column windowColumn = createWindowColumn(inputDataFrame, partitionField);
            windowsFields.put(partitionField.getAs(), windowColumn);
        }

        for (Map.Entry<String, Column> entry : windowsFields.entrySet()) {
            inputDataFrame = inputDataFrame.withColumn(entry.getKey(), entry.getValue());
        }

        transformDatasetService.persistDataset(stepExecution,DatasetRefs.fromTransform(transform), inputDataFrame);
    }

    private Column createWindowColumn(final Dataset<Row> dataFrame, PartitionField partitionField) {
        Column windowColumn;
        switch (partitionField.getFunction()) {
            case SUM:
                windowColumn = functions.sum(partitionField.getName());
                break;
            case COUNT:
                windowColumn = functions.count(partitionField.getName());
                break;
            case MAX:
                windowColumn = functions.max(partitionField.getName());
                break;
            case MIN:
                windowColumn = functions.min(partitionField.getName());
                break;
            case AVG:
                windowColumn = functions.avg(partitionField.getName());
                break;
            case PERCENT_RANK:
                windowColumn = functions.percent_rank();
                break;
            case DENSE_RANK:
                windowColumn = functions.dense_rank();
                break;
            case RANK:
                windowColumn = functions.rank();
                break;
            case ROW_NUMBER:
                windowColumn = functions.row_number();
                break;
            case FIRST:
                windowColumn = functions.first(partitionField.getName());
                break;
            case LAST:
                windowColumn = functions.last(partitionField.getName());
                break;
            default:
                throw new UnsupportedOperationException(MessageFormat.format("Not supported window function: {0}", partitionField.getFunction()));
        }
        WindowSpec windowSpec = null;
        if (CollectionUtils.isNotEmpty(partitionField.getPartitionBys())) {
            Column[] partitionFields = partitionField.getPartitionBys().stream()
                    .map(dataFrame::col)
                    .toArray(Column[]::new);
            windowSpec = Window.partitionBy(partitionFields);
        }

        if (CollectionUtils.isNotEmpty(partitionField.getOrderBys())) {
            Column[] orderByFields = partitionField.getOrderBys().stream()
                    .map(orderBy -> dataFrame.col(orderBy.getFieldName()))
                    .toArray(Column[]::new);
            if (windowSpec != null) {
                windowSpec.orderBy(orderByFields);
            } else {
                windowSpec = Window.orderBy(orderByFields);
            }
        }
        if (windowSpec != null) {
            windowColumn = windowColumn.over(windowSpec);
        } else {
            windowColumn = windowColumn.over();
        }

        return windowColumn;
    }

}
