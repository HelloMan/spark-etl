package etl.spark.pipeline.core;

import etl.api.job.pipeline.PipelineJobRequest;
import lombok.Getter;
import lombok.Setter;

import java.util.Date;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Domain object represent the execution of a job
 */
@Getter
@Setter
public class JobExecution {
	private long id;

	private PipelineJobRequest pipelineJobRequest;

	private JobExecutionContext jobExecutionContext;

	private ExecutionStatus executionStatus;

	private Exception exception;

	private Date startTime;

	private Date endTime;

	private List<StepExecution> stepExecutions = new CopyOnWriteArrayList<>();

	public JobExecution(long id,PipelineJobRequest jobRequest) {
		this.id = id;
		this.pipelineJobRequest = jobRequest;
		this.jobExecutionContext = new JobExecutionContext(this);
		executionStatus = ExecutionStatus.RUNNING;
	}
}
