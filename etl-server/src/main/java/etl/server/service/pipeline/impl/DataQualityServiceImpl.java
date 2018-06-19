package etl.server.service.pipeline.impl;

import etl.api.dq.DataQualityCheckExecution;
import etl.api.dq.DataQualityCheckState;
import etl.api.job.pipeline.PipelineJobRequest;
import etl.api.job.pipeline.PipelineStep;
import etl.common.hdfs.FileSystemTemplate;
import etl.server.domain.TransformationType;
import etl.server.domain.entity.ServiceRequest;
import etl.server.exception.dq.DQRuleInstanceStatusChangeException;
import etl.server.exception.dq.NoSuchRuleException;
import etl.server.repository.DQRuleInstanceRepository;
import etl.server.service.pipeline.DataQualityService;
import javaslang.Tuple;
import javaslang.Tuple2;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;

@Service
@Slf4j
public class DataQualityServiceImpl extends AbstractPipelineJobService implements DataQualityService {

	@Autowired
	private DQRuleInstanceRepository dqRuleInstanceRepository;

	@Autowired
	private FileSystemTemplate fileSystemTemplate;


	protected ServiceRequest createPipelineJob(PipelineJobRequest jobRequest) {
		ServiceRequest serviceRequest = super.createPipelineJob(jobRequest);
		jobRequest.getSteps().forEach(step -> createDataQualityCheck(serviceRequest, step));
		return serviceRequest;
	}

	private void createDataQualityCheck(ServiceRequest serviceRequest, PipelineStep step) {
		DataQualityCheckExecution instance = new DataQualityCheckExecution();
		instance.setJobExecutionId(serviceRequest.getId());
		instance.setName(step.getName());
		instance.setDescription(step.getDescription());
		instance.setStatus(DataQualityCheckState.ACCEPTED);
		dqRuleInstanceRepository.save(instance);
	}

	@Override
	public void updateDataQualityRuleExecution(long jobExecutionId, String ruleName, DataQualityCheckExecution execution)
			throws NoSuchRuleException, DQRuleInstanceStatusChangeException {
		DataQualityCheckExecution dqCheckExecution = dqRuleInstanceRepository.findByJobExecutionIdAndName(jobExecutionId, ruleName);
		if (dqCheckExecution == null) {
			throw new NoSuchRuleException(jobExecutionId, ruleName);
		}
		dqCheckExecution.setFailedRecordNumber(execution.getFailedRecordNumber());
		dqCheckExecution.setStartTime(execution.getStartTime());
		dqCheckExecution.setEndTime(execution.getEndTime());
		dqCheckExecution.setLastUpdateTime(execution.getLastUpdateTime());
		dqCheckExecution.setTargetFile(execution.getTargetFile());

		if (!dqCheckExecution.getStatus().canTransistTo(execution.getStatus())) {
			throw new DQRuleInstanceStatusChangeException(jobExecutionId, dqCheckExecution.getStatus(), execution.getStatus(), ruleName);
		}
		dqCheckExecution.setStatus(execution.getStatus());

		dqRuleInstanceRepository.save(dqCheckExecution);
	}

	@Override
	public List<DataQualityCheckExecution> getDataQualityRuleExecutions(long jobExecutionId)  {
		return dqRuleInstanceRepository.findByJobExecutionId(jobExecutionId);
	}

	@Override
	public Optional<Tuple2<String, InputStream>> getResultOfDataQualityRuleExecution(long jobExecutionId, String ruleName) throws IOException {
		DataQualityCheckExecution dataQualityCheckExecution = dqRuleInstanceRepository.findByJobExecutionIdAndName(jobExecutionId, ruleName);
		if (dataQualityCheckExecution == null) {
			return Optional.empty();
		}
		Path resultFile = new Path(dataQualityCheckExecution.getTargetFile());
	    try(InputStream inputStream = fileSystemTemplate.open(resultFile)){
			return Optional.of(Tuple.of(resultFile.getName(), inputStream));
		}
	}


	@Override
	public TransformationType getTransformationType() {
		return TransformationType.VALIDATION;
	}
}
