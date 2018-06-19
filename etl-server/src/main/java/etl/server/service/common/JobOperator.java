package etl.server.service.common;

import etl.api.job.JobExecution;
import etl.server.domain.entity.ServiceRequest;
import etl.server.exception.job.JobExecutionStopException;
import etl.server.exception.job.JobStartException;
import etl.server.exception.job.NoSuchJobExecutionException;

import java.util.Properties;
import java.util.function.Supplier;

public interface JobOperator {

	long startJob(String jobName, Supplier<ServiceRequest> serviceRequestSupplier, Properties properties)throws JobStartException;

	boolean stop(long executionId) throws JobExecutionStopException;

	JobExecution getJobExecution(Long executionId) throws NoSuchJobExecutionException;

}
