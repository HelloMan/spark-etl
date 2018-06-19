package etl.spark.staging.execution;

import etl.api.dataset.DatasetMetadata;
import etl.api.datasource.CsvDataSource;
import etl.api.job.staging.StagingItemRequest;
import etl.spark.TestStagingApplication;
import etl.spark.staging.datafields.DataRow;
import javaslang.control.Either;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = TestStagingApplication.class)
public class DatasetValidatorImplTest {

    private StagingItemRequest datasetValid;
    private StagingItemRequest datasetInvalid;
    private StagingItemRequest datasetHeader;

    @Autowired
    private DatasetValidator validator;

    @Autowired
    private DatasetLoader loader;

    @Before
    public void init() throws Exception {
        DatasetMetadata datasetMetadata = DatasetMetadata.of("valid_employee");
                // items
        datasetValid = StagingItemRequest.builder()
                .table("employee")
                .dataset(datasetMetadata)
                .source(CsvDataSource.builder().header(false).build())
                .build();
        datasetMetadata = DatasetMetadata.of("head_employee");
        datasetHeader = StagingItemRequest.builder()
                .table("employee")
                .dataset(datasetMetadata)
                .source(CsvDataSource.builder().header(true).build())
                .build();

        datasetMetadata = DatasetMetadata.of("invalid_employee");
        datasetInvalid = StagingItemRequest.builder()
                .table("employee")
                .dataset(datasetMetadata)
                .source(CsvDataSource.builder().header(false).build())
                .build();
    }



    @Test
    public void validDataset() throws Exception {
        JavaRDD<DataRow> rdd = loader.load(datasetValid);
        Either<JavaRDD<String>, Dataset<Row>> result = validator.validate(datasetValid,rdd);

        assertThat(result.isRight()).isTrue();
    }

    @Test
    public void datasetWithHeader() throws Exception {
        JavaRDD<DataRow> rdd = loader.load(datasetHeader);
        Either<JavaRDD<String>, Dataset<Row>> result = validator.validate(datasetHeader,rdd);
        assertThat(result.isRight()).isTrue();
        assertThat(result.get().count()).isEqualTo(3);
    }

    @Test
    public void execute_invalidDataset() throws Exception {
        JavaRDD<DataRow> rdd = loader.load(datasetInvalid);

        Either<JavaRDD<String>, Dataset<Row>> result = validator.validate(datasetInvalid,rdd);
        assertThat(result.isLeft()).isTrue();
        String expectedRes = "2,name2,30,female123,2010-02-03,Field [GENDER] expected to have a maximum length of 6 - actual: 9\n" +
				"3,name3,40a,m,2010-02-23,Field [AGE] expected numeric value - actual: 40a;Field [GENDER] expected to match the regex ^(male|female)$ - actual: m\n" +
				"<NULL>,name4,40,male,<NULL>,Field [ID] is a key, it can not be empty\n" +
				"5,name5,<NULL>,female,<NULL>,Field [AGE] is not nullable\n" +
				"6,name6,50000000000,male,02/02/2010,Field [AGE] expected to be a value between -2147483648 and 2147483647 - actual: 50000000000;Field [BIRTHDAY] expected to be a date with format yyyy-MM-dd - actual: 02/02/2010\n" +
				"7,name7,Expected 5 fields - actual: 2";

        String actualRes = result.getLeft().collect().stream().collect(Collectors.joining("\n"));
        assertThat(actualRes).isEqualTo(expectedRes);
    }

}