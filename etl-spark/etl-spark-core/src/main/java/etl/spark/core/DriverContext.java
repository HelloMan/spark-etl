package etl.spark.core;

import etl.api.job.JobType;
import org.apache.spark.sql.SparkSession;

public interface DriverContext {

    long getJobExecutionId();

    boolean isDebugMode();

    JobType getJobType();

    String getProperty(String key);

    int getInteger(String key);

    long getLong(String key);

    boolean getBoolean(String key);

    SparkSession getSparkSession();


}
