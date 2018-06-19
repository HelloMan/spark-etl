package etl.spark.pipeline.core;

import etl.api.job.pipeline.PipelineJobRequest;
import etl.api.job.pipeline.PipelineStep;
import etl.common.json.MapperWrapper;
import etl.spark.core.DriverContext;
import etl.spark.core.JobOperator;
import etl.spark.pipeline.transform.Interpreter;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;

/**
 * A template class which parse and  execute pipeline job
 */
public abstract class AbstractJobOperator implements JobOperator {

    @Autowired
    private SparkSession sparkSession;

    @Autowired
    private TransformDatasetService transformDatasetService;

    @Autowired
    private DriverContext driverContext;

    @Value("${job.content.file}")
    private String jobFile;


    public abstract JobListener getJobListener();

    public abstract StepListener getStepListener();

    public void runJob(PipelineJobRequest jobRequest) throws Exception {
        final JobExecution jobExecution = new JobExecution(driverContext.getJobExecutionId(), jobRequest);
        jobExecution.setStartTime(new Date());

        getJobListener().beforeJob(jobExecution);
        try {
            jobRequest.getSteps().forEach(pipelineStep -> runStep(jobExecution, pipelineStep));
            jobExecution.setExecutionStatus(ExecutionStatus.COMPLETED);
            jobExecution.setEndTime(new Date());
            getJobListener().afterJob(jobExecution);
        } catch (Exception e) {
            jobExecution.setExecutionStatus(ExecutionStatus.FAILED);
            jobExecution.setException(e);
            throwAsPipelineException(e);
            jobExecution.setEndTime(new Date());
            getJobListener().afterJob(jobExecution);
            throwAsPipelineException(e);
        }
    }
    
    public void runJob(String... args) throws Exception {
        this.runJob(getJobRequest());
    }

    private PipelineJobRequest getJobRequest() throws IOException {
        try( InputStream jobContent = this.getClass().getClassLoader().getResourceAsStream(jobFile)) {
            return MapperWrapper.MAPPER.readValue(jobContent, PipelineJobRequest.class);
        }
    }

    private void runStep(JobExecution jobExecution, PipelineStep pipelineStep) {
        final StepExecution stepExecution = StepExecution.builder()
                .step(pipelineStep)
                .jobExecution(jobExecution)
                .build();
        stepExecution.setStartTime(new Date());
        final Interpreter interpreter = new Interpreter(stepExecution, sparkSession, transformDatasetService);
        getStepListener().beforeStep(stepExecution);
        try {
            pipelineStep.getTransforms().forEach(transform -> transform.accept(interpreter));
            stepExecution.setExecutionStatus(ExecutionStatus.COMPLETED);
            stepExecution.setEndTime(new Date());
            getStepListener().afterStep(stepExecution);
        } catch (Exception e) {
            stepExecution.setException(e);
            stepExecution.setExecutionStatus(ExecutionStatus.FAILED);
            stepExecution.setEndTime(new Date());
            getStepListener().afterStep(stepExecution);
            throwAsPipelineException(e);
        }
    }

    private void throwAsPipelineException(Exception e) {
        if (e instanceof PipelineException) {
            throw (PipelineException) e;
        } else {
            throw new PipelineException(e);
        }
    }
}
