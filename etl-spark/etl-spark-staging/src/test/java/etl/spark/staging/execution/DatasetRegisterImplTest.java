package etl.spark.staging.execution;

import com.google.common.collect.ImmutableList;
import etl.api.dataset.DatasetMetadata;
import etl.api.job.staging.StagingItemRequest;
import etl.spark.TestStagingApplication;
import etl.spark.util.DatasetRow;
import javaslang.control.Either;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;



@RunWith(SpringRunner.class)
@SpringBootTest(classes = TestStagingApplication.class)
public class DatasetRegisterImplTest {

    @Autowired
    private DatasetRegister register;

    @Autowired
    private SparkSession spark;



    @Test
    public void saveValidDataset() {
        StagingItemRequest stagingItemRequest = StagingItemRequest.builder()
                .table("employee")
                .id(1l)
                .dataset(DatasetMetadata.of("employee"))
                .build();

        StructField field1 = new StructField("id", DataTypes.IntegerType, true, Metadata.empty());
        StructField field2 = new StructField("name", DataTypes.StringType, true, Metadata.empty());
        StructField[] fields = {field1, field2};
        StructType structType = new StructType(fields);

        List<Row> data = ImmutableList.of(
                RowFactory.create(1, "a1")
        );
        Dataset<Row> df = spark.createDataFrame(data, structType);


        register.register(stagingItemRequest, Either.right(df));

        List<Row> rows = spark.sql("select * from Employee").collectAsList();
        Map<String, Object> map = new DatasetRow(rows.get(0)).asMap();
        assertThat(map.size()).isEqualTo(3);
    }
}