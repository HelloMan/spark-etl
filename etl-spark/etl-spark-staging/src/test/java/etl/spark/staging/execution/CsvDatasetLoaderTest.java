package etl.spark.staging.execution;

import etl.api.dataset.DatasetMetadata;
import etl.api.datasource.CsvDataSource;
import etl.api.job.staging.StagingItemRequest;
import etl.spark.TestStagingApplication;
import etl.spark.staging.datafields.DataRow;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static org.assertj.core.api.Assertions.assertThat;
@RunWith(SpringRunner.class)
@SpringBootTest(classes = TestStagingApplication.class)
public class CsvDatasetLoaderTest {

    @Autowired
    private DatasetLoader loader;
    @Test
    public void testLoad() throws Exception {
         StagingItemRequest stagingItemRequest = StagingItemRequest.builder()
                .table("employee")
                .id(1l)
                 .source(CsvDataSource.builder().header(false).build())
                .dataset(DatasetMetadata.of("valid_employee"))
                .build();


         JavaRDD<DataRow> result = loader.load(stagingItemRequest);

        assertThat(result.collect().size()).isEqualTo(3);
    }
}