package etl.spark.pipeline.dq;

import etl.api.dq.DataQualityCheckExecution;
import etl.api.dq.DataQualityCheckState;
import etl.api.job.pipeline.PipelineJobRequest;
import etl.api.job.pipeline.PipelineStep;
import etl.api.pipeline.DatasetRef;
import etl.api.pipeline.Transform;
import etl.client.DataQualityClient;
import etl.spark.core.DriverContext;
import etl.spark.pipeline.core.ExecutionStatus;
import etl.spark.pipeline.core.PipelineException;
import etl.spark.pipeline.core.StepExecution;
import etl.spark.pipeline.core.StepListener;
import etl.spark.pipeline.core.TransformDatasetService;
import javaslang.collection.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class DataQualityStepListener implements StepListener {


	@Autowired
	private TransformDatasetService transformDatasetService;

	@Autowired
	private DriverContext driverContext;
	@Autowired
	private DataQualityClient dataQualityUpdateClient;

	@Override
	public void beforeStep(StepExecution stepExecution) {
		PipelineStep step = stepExecution.getStep();
		PipelineJobRequest jobRequest = stepExecution.getJobExecution().getPipelineJobRequest();
		log.info("Start DQ step: {} in Job: {}", step.getName(), jobRequest.getName());

		// update status
		final long jobExecutionId = driverContext.getJobExecutionId();
		DataQualityCheckExecution execution = createDataQualityRuleExecution(stepExecution, jobExecutionId);
		execution.setStartTime(stepExecution.getStartTime());
		execution.setLastUpdateTime(stepExecution.getStartTime());
		DataQualityCheckState ruleStatus = DataQualityCheckState.CHECKING;
		execution.setStatus(ruleStatus);
		dataQualityUpdateClient.updateDQRuleCheckExecution(execution.getJobExecutionId(),execution.getName(),execution);
	}

	private DataQualityCheckExecution createDataQualityRuleExecution(StepExecution stepExecution, long jobExecutionId) {
		DataQualityCheckExecution execution = new DataQualityCheckExecution();
		execution.setJobExecutionId(jobExecutionId);
		execution.setName(stepExecution.getName());
		return execution;
	}

	@Override
	public void afterStep(StepExecution stepExecution) {
		PipelineStep step = stepExecution.getStep();
		log.info("Finished DQ step: {} in Job: {}", step.getName(), stepExecution.getJobExecution().getPipelineJobRequest().getName());

		// update status
		final long jobExecutionId = driverContext.getJobExecutionId();
		DataQualityCheckExecution execution = createDataQualityRuleExecution(stepExecution, jobExecutionId);
		execution.setEndTime(stepExecution.getEndTime());
		execution.setLastUpdateTime(stepExecution.getEndTime());

		boolean executionCompleted = stepExecution.getExecutionStatus() == ExecutionStatus.COMPLETED;
		DataQualityCheckState status = executionCompleted ? DataQualityCheckState.CHECKED : DataQualityCheckState.CHECK_FAILED;
		execution.setStatus(status);

		if (executionCompleted) {
			execution.setFailedRecordNumber(getRecordCount(stepExecution));
		}
		dataQualityUpdateClient.updateDQRuleCheckExecution(execution.getJobExecutionId(),execution.getName(),execution);
	}





	private Transform getLastTransform(StepExecution stepExecution) {
		return List.ofAll(stepExecution.getStep().getTransforms())
				.lastOption()
				.getOrElseThrow(() -> new PipelineException(
						String.format("Transform not defined in step %s", stepExecution.getName())));
	}


	private DatasetRef getTransformOutput(StepExecution stepExecution) {
		Transform lastTransform = getLastTransform(stepExecution);
		return lastTransform.getTransformOutputs().stream().findFirst().orElseThrow(
				() -> new PipelineException(
						String.format("TransformOutput not defined in transform %s", lastTransform)));
	}

	private long getRecordCount(StepExecution stepExecution) {
		final DatasetRef datasetRef = getTransformOutput(stepExecution);
		return transformDatasetService.getDataset(stepExecution, datasetRef)
				.map(Dataset::count)
				.orElse(0L);
	}
}