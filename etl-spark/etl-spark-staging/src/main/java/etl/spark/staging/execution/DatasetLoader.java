package etl.spark.staging.execution;

import etl.api.job.staging.StagingItemRequest;
import etl.spark.staging.datafields.DataRow;
import org.apache.spark.api.java.JavaRDD;

@FunctionalInterface
public interface DatasetLoader {
    JavaRDD<DataRow> load(StagingItemRequest itemRequest);
}