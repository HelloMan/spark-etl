package etl.spark.pipeline.transform.parser;

import com.google.common.collect.FluentIterable;
import etl.api.pipeline.DatasetRefs;
import etl.api.pipeline.Distinct;
import etl.spark.pipeline.core.PipelineException;
import etl.spark.pipeline.core.StepExecution;
import etl.spark.pipeline.core.TransformDatasetService;
import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class DistinctParser extends AbstractTransformParser<Distinct> {


    public DistinctParser(TransformDatasetService transformDatasetService,StepExecution stepExecution) {
        super(transformDatasetService, stepExecution);
    }

    @Override
    public void parse(Distinct transform) {
        final Dataset<Row> inputDataset = transformDatasetService.getDataset(stepExecution,transform.getInput())
                .orElseThrow(() -> new PipelineException(String.format("Dataset can not be found for transform's input %s", transform.getInput())));
        Dataset<Row> result;
        if (CollectionUtils.isNotEmpty(transform.getFields())) {
            String[] distinctFields = FluentIterable.from(transform.getFields()).toArray(String.class);
            result = inputDataset.dropDuplicates(distinctFields);
        } else {
            result = inputDataset.distinct();
        }
        transformDatasetService.persistDataset(stepExecution,DatasetRefs.fromTransform(transform), result);
    }
}
