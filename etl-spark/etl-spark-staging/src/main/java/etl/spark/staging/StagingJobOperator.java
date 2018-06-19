package etl.spark.staging;

import etl.api.job.JobType;
import etl.api.job.staging.StagingJobRequest;
import etl.common.json.MapperWrapper;
import etl.spark.core.JobOperator;
import etl.spark.staging.execution.DatasetStagingWorker;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;

@Component
public class StagingJobOperator implements JobOperator {

    @Autowired
    private DatasetStagingWorker datasetStagingWorker;

    @Value("${job.content.file}")
    private String jobFile;


    @Override
    public void runJob(String... args) throws Exception {
        StagingJobRequest jobRequest = getJobRequest();
        runJob(jobRequest);
    }

    protected void runJob(StagingJobRequest jobRequest) {
        jobRequest.getItems().forEach(datasetStagingWorker::execute);
    }

    private StagingJobRequest getJobRequest() throws IOException {
        try (InputStream jobContent = this.getClass().getClassLoader().getResourceAsStream(jobFile)) {
            return MapperWrapper.MAPPER.readValue(jobContent, StagingJobRequest.class);
        }
    }

    @Override
    public JobType getJobType() {
        return JobType.STAGING_DATASET;
    }
}