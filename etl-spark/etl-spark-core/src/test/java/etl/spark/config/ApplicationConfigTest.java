package etl.spark.config;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
public class ApplicationConfigTest {

    @Test
    public void testSparkSession() throws Exception {

        ApplicationConfig applicationConfig = new ApplicationConfig();
        applicationConfig.setDebugMode(true);
        assertThat(applicationConfig.sparkSession()).isNotNull();
    }

    @Test
    public void testJavaSparkContext() throws Exception {
        ApplicationConfig applicationConfig = new ApplicationConfig();
        applicationConfig.setDebugMode(true);

        assertThat(applicationConfig.javaSparkContext(applicationConfig.sparkSession())).isNotNull();
    }
}