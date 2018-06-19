package etl.spark.pipeline.dq;

import etl.spark.core.DriverContext;
import etl.spark.TestPipelineApplication;
import etl.spark.pipeline.transform.TransformUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = TestPipelineApplication.class)
public class DataQualityJobOperatorTest {

    @Autowired
    private DataQualityJobOperator jobOperator;

    @Autowired
    private DriverContext driverContext;

    @Test
    public void testRun() throws Exception {
        jobOperator.runJob(TransformUtil.buildPipeline());
        //assert
        assertThat(driverContext.getSparkSession().sql("select * from outputResult").count()).isEqualTo( 9);
        assertThat(driverContext.getSparkSession().sql("select Country,count(*) from outputResult group by Country").count()).isEqualTo(3);
	}


}