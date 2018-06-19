package etl.server.service.staging.impl.job;

import etl.server.domain.entity.ServiceRequestType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.transaction.PlatformTransactionManager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class StagingJobConfigurationTest {
    @Mock
    private JobBuilderFactory jobs ;

    @Mock
    private StepBuilderFactory steps;
    @InjectMocks
    StagingJobConfiguration jobConfiguration;

    @Before
    public void setup(){
        JobRepository jobRepository = mock(JobRepository.class);
        JobBuilder jobBuilder = new JobBuilder("stagingjob").repository(jobRepository);

        when(jobs.get(ServiceRequestType.STAGING.name())).thenReturn(jobBuilder);

        PlatformTransactionManager platformTransactionManager = mock(PlatformTransactionManager.class);
        StepBuilder stepBuilder =  new StepBuilder("upload").repository(jobRepository).transactionManager(
                platformTransactionManager);
        when(steps.get(anyString())).thenReturn(stepBuilder);

    }


    @Test
    public void stagingJob() throws Exception {
        org.springframework.batch.core.Job job = jobConfiguration.stagingJob();
        assertThat(job).isNotNull();
    }

    @Test
    public void uploadStep() throws Exception {
        Step step = jobConfiguration.uploadStep();
        assertThat(step).isNotNull();
    }

    @Test
    public void validateStep() throws Exception {
        Step step = jobConfiguration.validateStep();
        assertThat(step).isNotNull();
    }

    @Test
    public void uploadTaskLet() throws Exception {
        assertThat(jobConfiguration.uploadTaskLet()).isNotNull();
    }

    @Test
    public void validateTaskLet() throws Exception {
        assertThat(jobConfiguration.validateTaskLet()).isNotNull();
    }

}