package etl.spark.pipeline.dq;

import etl.spark.pipeline.core.JobExecution;
import etl.spark.pipeline.core.JobListener;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class DataQualityJobListener implements JobListener {

	@Override
	public void beforeJob(JobExecution jobExecution) {
	}

	@Override
	public void afterJob(JobExecution jobExecution) {
	}

}