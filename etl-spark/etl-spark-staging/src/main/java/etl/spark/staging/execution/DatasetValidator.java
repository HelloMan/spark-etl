package etl.spark.staging.execution;

import etl.api.job.staging.StagingItemRequest;
import etl.spark.staging.datafields.DataRow;
import javaslang.control.Either;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

@FunctionalInterface
public interface DatasetValidator {
    Either<JavaRDD<String>, Dataset<Row>> validate(StagingItemRequest item,JavaRDD<DataRow> srcRDD);
}