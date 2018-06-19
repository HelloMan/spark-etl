package etl.server.config;

import etl.server.IgnisApplication;
import org.apache.spark.SparkConf;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(SpringRunner.class)
@AutoConfigureMockMvc
@SpringBootTest(classes = IgnisApplication.class)
public class SparkConfigurationIT {


    @Autowired
    private SparkConf sparkConf;
    @Test
    public void testSparkconf() throws Exception {
        assertThat(sparkConf).isNotNull();
        assertThat(sparkConf.get("spark.sql.warehouse.dir")).isEqualTo("/user/etl/warehouse");

    }
}