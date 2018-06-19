package etl.spark.staging.config;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.core.env.Environment;

import static org.assertj.core.api.Assertions.assertThat;
@RunWith(MockitoJUnitRunner.class)
public class StagingConfigTest {

    @Mock
    Environment environment;
    @InjectMocks
    StagingConfig stagingConfig;
    @Test
    public void testGetDatasetRootPath() throws Exception {

        Mockito.when(environment.getProperty("dataset.path")).thenReturn("stagingPath");
        assertThat(stagingConfig.getDatasetRootPath()).isEqualTo("stagingPath");
    }


    public static void main(String[] args) {

    }
}