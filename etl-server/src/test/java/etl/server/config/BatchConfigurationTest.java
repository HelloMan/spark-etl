package etl.server.config;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;

import static org.assertj.core.api.Assertions.assertThat;


@RunWith(MockitoJUnitRunner.class)
public class BatchConfigurationTest {


    @Mock
    private JobRepository jobRepository;

    @InjectMocks
    BatchConfiguration batchConfiguration;
    @Test
    public void jobLauncher() throws Exception {
        JobLauncher jobLauncher = batchConfiguration.jobLauncher();
        assertThat(jobLauncher).isNotNull();

    }

    @Test
    public void jobRegistryBeanPostProcessor() throws Exception {
        JobRegistry jobRegistry = Mockito.mock(JobRegistry.class);
        assertThat(batchConfiguration.jobRegistryBeanPostProcessor(jobRegistry)).isNotNull();
    }


}