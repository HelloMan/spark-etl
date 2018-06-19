package etl.server.service.pipeline.impl.job;

import etl.server.domain.TransformationType;
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
public class DataQualityJobConfigurationTest {

	@Mock
	private JobBuilderFactory jobs;

	@Mock
	private StepBuilderFactory steps;

	@InjectMocks
	private DataQualityJobConfiguration dataQualityJobConfiguration;

	@Before
	public void setup() {
		JobRepository jobRepository = mock(JobRepository.class);
		JobBuilder jobBuilder = new JobBuilder("s").repository(jobRepository);

		when(jobs.get(TransformationType.VALIDATION.name())).thenReturn(jobBuilder);

		PlatformTransactionManager platformTransactionManager = mock(PlatformTransactionManager.class);
		StepBuilder stepBuilder = new StepBuilder("test").repository(jobRepository).transactionManager(
				platformTransactionManager);
		when(steps.get(anyString())).thenReturn(stepBuilder);

	}

	@Test
	public void dataQualityJob() throws Exception {
		org.springframework.batch.core.Job job = dataQualityJobConfiguration.dataQualityJob();
		assertThat(job).isNotNull();

	}

	@Test
	public void dqCheckStep() throws Exception {
		Step step = dataQualityJobConfiguration.dqCheckStep();
		assertThat(step).isNotNull();
	}

	@Test
	public void pipelineTasklet() throws Exception {
		assertThat(dataQualityJobConfiguration.pipelineTasklet()).isNotNull();
	}

}