package etl.server.service.common;

import etl.api.job.JobExecution;
import etl.server.domain.entity.ServiceRequest;
import etl.server.exception.job.JobExecutionStopException;
import etl.server.exception.job.JobStartException;
import etl.server.repository.ServiceRequestRepository;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobExecutionNotRunningException;
import org.springframework.batch.core.launch.JobInstanceAlreadyExistsException;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.launch.NoSuchJobExecutionException;
import org.springframework.batch.support.PropertiesConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Properties;
import java.util.function.Supplier;


@Component
public class JobOperatorImpl implements JobOperator {

	@Autowired
	private org.springframework.batch.core.launch.JobOperator jobOperator;

	@Autowired
	private JobExplorer jobExplorer;

	@Autowired
	private ServiceRequestRepository serviceRequestRepository;

	@Override
	public long startJob(String jobName, Supplier<ServiceRequest> serviceRequestSupplier, Properties properties)
			throws JobStartException {
		ServiceRequest serviceRequest = serviceRequestSupplier.get();
		properties.setProperty(ServiceRequest.SERVICE_REQUEST_ID, String.valueOf(serviceRequest.getId()));
		try {
			long jobExecutionId = jobOperator.start(jobName, PropertiesConverter.propertiesToString(properties));
			serviceRequest.setJobExecutionId(jobExecutionId);
			serviceRequestRepository.save(serviceRequest);
			return serviceRequest.getId();
		} catch (NoSuchJobException | JobInstanceAlreadyExistsException | JobParametersInvalidException e) {
			throw new JobStartException(e);
		}
	}

	@Override
	public boolean stop(long executionId) throws JobExecutionStopException {
		ServiceRequest serviceRequest = serviceRequestRepository.findOne(executionId);
		try {
			return jobOperator.stop(serviceRequest.getJobExecutionId());
		} catch (NoSuchJobExecutionException | JobExecutionNotRunningException e) {
			throw new JobExecutionStopException(e);
		}
	}

	@Override
	public JobExecution getJobExecution(Long executionId) throws etl.server.exception.job.NoSuchJobExecutionException {
		ServiceRequest serviceRequest = serviceRequestRepository.findOne(executionId);
		return toResponse(jobExplorer.getJobExecution(serviceRequest.getJobExecutionId()));
	}
	private   JobExecution toResponse(org.springframework.batch.core.JobExecution jobExecution) {
		return JobExecution.builder()
				.jobExecutionId(jobExecution.getId())
				.exitStatus(jobExecution.getExitStatus().getExitCode())
				.jobStatus(jobExecution.getStatus().name().toLowerCase())
				.startTime(jobExecution.getStartTime())
				.finishTime(jobExecution.getEndTime())
				.build();
	}



}
