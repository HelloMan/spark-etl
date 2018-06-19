package etl.spark.pipeline.transform.parser;

import etl.api.pipeline.DatasetRef;
import etl.api.pipeline.DatasetRefs;
import etl.api.pipeline.Union;
import etl.spark.pipeline.core.StepExecution;
import etl.spark.pipeline.core.TransformDatasetService;
import etl.spark.pipeline.core.PipelineException;

public class UnionParser extends AbstractTransformParser<Union> {


    public UnionParser(TransformDatasetService transformDatasetService,StepExecution stepExecution) {
        super(transformDatasetService,stepExecution);
    }

    @Override
    public void parse(Union operator) {
        org.apache.spark.sql.Dataset result = null;
        for (DatasetRef input : operator.getInputs()) {
            org.apache.spark.sql.Dataset dataFrame = transformDatasetService.getDataset(stepExecution,input)
                    .orElseThrow(() -> new PipelineException(String.format("Dataset can not be found for transform's input %s", input)));
            if (result == null) {
                result = dataFrame;
            } else {
                result = result.union(dataFrame);
            }
        }
        transformDatasetService.persistDataset(stepExecution,DatasetRefs.fromTransform(operator), result);
    }

}