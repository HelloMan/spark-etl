package etl.server.service.pipeline.impl.job;

import etl.server.config.BatchConfiguration;
import etl.server.domain.TransformationType;
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
 * Job configuration for data quality job
 */
@Configuration
@Import(BatchConfiguration.class)
public class DataQualityJobConfiguration {

	@Autowired
	private JobBuilderFactory jobs;

	@Autowired
	private StepBuilderFactory steps;

	@Bean
	public Job dataQualityJob() {
		return jobs.get(TransformationType.VALIDATION.name())
				.incrementer(new RunIdIncrementer())
				.start(dqCheckStep())
				.build();
	}

	@Bean
	public Step dqCheckStep() {
		return steps.get("dqCheck")
				.tasklet(pipelineTasklet())
				.transactionAttribute(BatchConfiguration.NOT_SUPPORTED_TRANSACTION)
				.build();
	}

	@Bean
	@StepScope
	public Tasklet pipelineTasklet() {
		return new DataQualityCheckTasklet();
	}

}