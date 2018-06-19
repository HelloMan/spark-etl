package etl.server.config;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.core.env.Environment;

import static org.assertj.core.api.Assertions.assertThat;
@RunWith(MockitoJUnitRunner.class)
public class HadoopConfigurationTest {

    @InjectMocks
    private HadoopBean hadoopBean;

    @Mock
    Environment environment;



    @Test
    public void shouldPassAndConfigurationNotNull() throws Exception {
        Configuration configuration = hadoopBean.hdfsConfiguration();
        assertThat(configuration).isNotNull();
    }
}