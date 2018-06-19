package etl.spark.staging.execution;

import etl.api.job.staging.StagingItemRequest;
import etl.spark.staging.datafields.DataRow;
import javaslang.control.Either;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class DatasetStagingWorker {
    @Autowired
    private  DatasetLoader loader;
    @Autowired
    private DatasetValidator validator;
    @Autowired
    private DatasetRegister register;

    public void execute(StagingItemRequest itemRequest) {
        JavaRDD<DataRow> srcRDD = loader.load(itemRequest);
        Either<JavaRDD<String>, Dataset<Row>> validateResult = validator.validate(itemRequest,srcRDD);
        register.register(itemRequest,validateResult);
    }
}