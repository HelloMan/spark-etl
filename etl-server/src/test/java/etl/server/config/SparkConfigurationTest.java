package etl.server.config;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.core.env.Environment;

import static org.assertj.core.api.Assertions.assertThat;


@RunWith(MockitoJUnitRunner.class)
public class SparkConfigurationTest {

    @InjectMocks
    private SparkBean sparkBean;

    @Mock
    Environment environment;

    @Test
    public void sparkconf() throws Exception {
        sparkBean.setSparkConfDir(this.getClass().getClassLoader().getResource("spark/conf").getPath());

        assertThat(sparkBean.sparkconf()).isNotNull();
    }

}