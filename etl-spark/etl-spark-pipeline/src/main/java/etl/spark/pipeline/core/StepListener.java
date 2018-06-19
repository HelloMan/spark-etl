package etl.spark.pipeline.core;

public interface StepListener {

    default void beforeStep(StepExecution stepExecution) {}

    default void afterStep(StepExecution stepExecution) {}

}
