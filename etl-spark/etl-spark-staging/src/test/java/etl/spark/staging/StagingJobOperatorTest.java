package etl.spark.staging;

import etl.api.job.staging.StagingJobRequest;
import etl.common.json.MapperWrapper;
import etl.spark.TestStagingApplication;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)

@SpringBootTest(classes = TestStagingApplication.class)
public class StagingJobOperatorTest {

    @Autowired
    private StagingJobOperator jobOperator;
    @Test
    public void testDriver() throws Exception {
        StagingJobRequest stagingJobRequest = MapperWrapper.MAPPER
                .readValue(this.getClass().getClassLoader().getResourceAsStream("job.json"), StagingJobRequest.class);
        jobOperator.runJob(stagingJobRequest);
    }
}
