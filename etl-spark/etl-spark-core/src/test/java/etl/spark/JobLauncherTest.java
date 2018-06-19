package etl.spark;

import etl.spark.core.DriverContext;
import etl.spark.core.JobOperator;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
@RunWith(SpringRunner.class)
@SpringBootTest(classes = TestApplication.class)
public class JobLauncherTest {

    @Autowired
    private DriverContext driverContext;

    @Autowired
    private List<JobOperator> jobOperatorList;
    @Test
    public void testRun() throws Exception {
        JobLauncher jobLauncher = new JobLauncher();
        jobLauncher.setDriverContext(driverContext);
        jobLauncher.setJobOperators(jobOperatorList);

        jobLauncher.run(new String[]{"hello world", "testTable"});
        assertThat(driverContext.getSparkSession().sql("select * from testTable").count()).isEqualTo(1);

    }
}