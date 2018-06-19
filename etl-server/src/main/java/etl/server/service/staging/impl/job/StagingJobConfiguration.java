package etl.server.service.staging.impl.job;

import etl.server.config.BatchConfiguration;
import etl.server.domain.entity.ServiceRequestType;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * Job configuration for staging dataset
 */
@Configuration
@Import(BatchConfiguration.class)
public class StagingJobConfiguration {

	@Autowired
	private JobBuilderFactory jobs;

	@Autowired
	private StepBuilderFactory steps;
	@Bean
	public Job stagingJob() {
		return jobs.get(ServiceRequestType.STAGING.name())
				.incrementer(new RunIdIncrementer())
				.start(uploadStep())
				.on(ExitStatus.NOOP.getExitCode()).end()
				.on(ExitStatus.COMPLETED.getExitCode()).to(validateStep())
				.build()
				.build();
	}

	@Bean
	public Step uploadStep() {
		return steps.get("upload")
				.tasklet(uploadTaskLet())
				.transactionAttribute(BatchConfiguration.NOT_SUPPORTED_TRANSACTION)
				.build();
	}

	@Bean
	public Step validateStep() {
		return steps.get("validate")
				.tasklet(validateTaskLet())
				.transactionAttribute(BatchConfiguration.NOT_SUPPORTED_TRANSACTION)
				.build();
	}

	@Bean
	@StepScope
	public Tasklet uploadTaskLet() {
		return new DatasetUploadTasklet();
	}

	@Bean
	@StepScope
	public Tasklet validateTaskLet() {
		return new DatasetStagingTasklet();
	}

}
