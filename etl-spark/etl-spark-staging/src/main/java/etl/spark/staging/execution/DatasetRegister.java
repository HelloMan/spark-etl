package etl.spark.staging.execution;

import etl.api.job.staging.StagingItemRequest;
import javaslang.control.Either;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

@FunctionalInterface
public interface DatasetRegister {
    void register(StagingItemRequest itemRequest,Either<JavaRDD<String>, Dataset<Row>> validateResult);
}