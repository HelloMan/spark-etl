package etl.spark.pipeline.transform.parser;

import etl.api.pipeline.Transform;
import etl.spark.pipeline.core.StepExecution;
import etl.spark.pipeline.core.TransformDatasetService;

public abstract class AbstractTransformParser<T extends Transform> implements TransformParser<T> {

    protected final TransformDatasetService transformDatasetService;

    protected final StepExecution stepExecution;

    public AbstractTransformParser(TransformDatasetService transformDatasetService,StepExecution stepExecution) {
        this.transformDatasetService = transformDatasetService;
        this.stepExecution = stepExecution;
    }
}
