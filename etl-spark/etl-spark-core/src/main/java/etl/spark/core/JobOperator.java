package etl.spark.core;

import etl.api.job.JobType;

public interface JobOperator {

    void runJob(String... args) throws Exception;

    JobType getJobType();
}
