package etl.spark.staging.execution;

import etl.api.job.staging.StagingItemRequest;
import etl.api.table.Table;
import etl.client.TableClient;
import etl.spark.staging.datafields.DataRow;
import com.machinezoo.noexception.Exceptions;
import javaslang.control.Either;
import javaslang.control.Validation;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
@Component
public class DatasetValidatorImpl implements DatasetValidator,Serializable{

    @Autowired
    private transient StagingListener listener;

    @Autowired
    private transient SparkSession sparkSession;

    @Autowired
    private transient TableClient tableClient;

    @Override
    public Either<JavaRDD<String>, Dataset<Row>> validate(StagingItemRequest item,JavaRDD<DataRow> srcRDD) {
        listener.onValidationStart(item.getId());
        Table table = Exceptions.sneak().get(() -> tableClient.getTable(item.getTable()).execute().body());
        DatasetSchema datasetSchema = new DatasetSchema(table);
        final JavaRDD<String> invalidRDD = validateRdd(srcRDD, datasetSchema);
        if (invalidRDD.isEmpty()) {
            final JavaRDD<Row> javaRDD = srcRDD.map(dataRow -> toStructRow(dataRow, datasetSchema));
            final Dataset<Row> validDataset = sparkSession.createDataFrame(javaRDD, datasetSchema.getStructType());
            return Either.right(validDataset);

        } else {
            return Either.left(invalidRDD);
        }
    }

    private JavaRDD<String> validateRdd(JavaRDD<DataRow> srcRDD, DatasetSchema schema) {
        return srcRDD.map(r -> this.validateRow(r, schema)).filter(Optional::isPresent).map(Optional::get);
    }


    private Optional<String> validateRow(DataRow fields, DatasetSchema datasetSchema) {
        Validation<String, DataRow> rowValidation = datasetSchema.validate(fields);
        if (rowValidation.isInvalid()) {
            return Optional.of(errorMessage(fields, rowValidation.getError()));
        }
        return Optional.empty();
    }

    private Row toStructRow(DataRow dataRow, DatasetSchema datasetSchema) {
        final Object[] values = dataRow.getFields().stream()
                .map(datasetSchema::parseField)
                .toArray(Object[]::new);
        return RowFactory.create(values);
    }



    private String errorMessage(DataRow dataRow, String result) {
        final String row = dataRow.getFields().stream()
                .map(f -> StringUtils.isBlank(f.getValue()) ? "<NULL>" : f.getValue())
                .collect(Collectors.joining(","));
        return row + "," + (StringUtils.isBlank(result) ? "" : result);
    }
}