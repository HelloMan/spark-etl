package etl.spark.pipeline.core;

public interface JobListener   {

    default void beforeJob(JobExecution jobExecution){}

    default void afterJob(JobExecution jobExecution){}

}
