package etl.spark.pipeline.transform.parser;

import etl.api.pipeline.DatasetRefs;
import etl.api.pipeline.Split;
import etl.spark.pipeline.core.PipelineException;
import etl.spark.pipeline.core.StepExecution;
import etl.spark.pipeline.core.TransformDatasetService;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class SplitParser extends AbstractTransformParser<Split> {


    public SplitParser(TransformDatasetService transformDatasetService,StepExecution stepExecution) {
        super(transformDatasetService,stepExecution);
    }

    @Override
    public void parse(Split transform) {
        Dataset<Row> inputDataset = transformDatasetService.getDataset(stepExecution,transform.getInput())
                .orElseThrow(() -> new PipelineException(String.format("Dataset can not be found for transform's input %s", transform.getInput())));
        Dataset<Row> output1 = inputDataset.filter(transform.getCondition());
        Dataset<Row> output2 = inputDataset.filter(String.format("!(%s)", transform.getCondition()));
        transformDatasetService.persistDataset(stepExecution,DatasetRefs.fromTransform(transform,true), output1);
        transformDatasetService.persistDataset(stepExecution,DatasetRefs.fromTransform(transform,false), output2);
    }
}