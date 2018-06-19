package etl.spark.pipeline.core;

import etl.api.job.pipeline.PipelineStep;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.Date;

/**
 * Domain object represent the execution of a step
 */

@Getter
@Setter
public class StepExecution {

	private final JobExecution jobExecution;

	private final PipelineStep step;

	private ExecutionStatus executionStatus;

	private Exception exception;

	private Date startTime;

	private Date endTime;

	@Builder
	public StepExecution(@NonNull JobExecution jobExecution, @NonNull PipelineStep step) {
		this.jobExecution = jobExecution;
		this.step = step;
		jobExecution.getStepExecutions().add(this);
		executionStatus = ExecutionStatus.RUNNING;
	}

	public String getName() {
		return step.getName();
	}

	public JobExecutionContext getJobExecutionContext() {
		return jobExecution.getJobExecutionContext();
	}

}
