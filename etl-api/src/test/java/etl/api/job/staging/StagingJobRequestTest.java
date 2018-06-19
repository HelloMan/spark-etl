package etl.api.job.staging;

import com.fasterxml.jackson.databind.ObjectMapper;
import etl.api.dataset.DatasetMetadata;
import etl.api.datasource.CsvDataSource;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
public class StagingJobRequestTest {

    private StagingJobRequest stagingJobRequest;

    @Before
    public void setup() throws Exception {
        stagingJobRequest = createStagingJobRequest();
    }
    @Test
    public void testBuilder() {

        Assert.assertEquals(stagingJobRequest.getName(), "staging");
        Assert.assertEquals(stagingJobRequest.getItems().size(), 2);
        Assert.assertTrue(stagingJobRequest.getItems().iterator().next().getSource() instanceof CsvDataSource);
        CsvDataSource csvSource = (CsvDataSource) stagingJobRequest.getItems().iterator().next().getSource();

        Assert.assertEquals(csvSource.getDelimiter(), ',');
        Assert.assertEquals(csvSource.getFilePath(), "/path/to/local1.csv");
        Assert.assertTrue(!csvSource.isHeader());

    }

    private StagingJobRequest createStagingJobRequest() throws Exception {
        CsvDataSource csvSource1 = CsvDataSource.builder().filePath("/path/to/local1.csv").delimiter(',').header(false).build();
        CsvDataSource csvSource2 = CsvDataSource.builder().filePath("/path/to/local2.csv").delimiter(',').header(false).build();

        StagingItemRequest stagingItem1 = StagingItemRequest.builder().source(csvSource1) .dataset(DatasetMetadata.of("1_employee")) .build();

        StagingItemRequest stagingItem2 = StagingItemRequest.builder().source(csvSource2) .dataset(DatasetMetadata.of("2_employee")).build();
        return StagingJobRequest.builder().name("staging").item(stagingItem1).item(stagingItem2).build();
    }

    @Test
    public void testStagintItemJsonBuilder() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(stagingJobRequest);
        StagingJobRequest jobRequest = mapper.readValue(json, StagingJobRequest.class);
        assertThat(jobRequest.getName()).isEqualTo(stagingJobRequest.getName());
        assertThat(jobRequest.getItems().size()).isEqualTo(stagingJobRequest.getItems().size());

    }
}
