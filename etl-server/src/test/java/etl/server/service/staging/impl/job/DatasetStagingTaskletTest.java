package etl.server.service.staging.impl.job;

import etl.server.util.spark.SparkSubmitOption;
import etl.server.service.pipeline.impl.job.PipelineConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.internal.util.reflection.Whitebox;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.scope.context.StepContext;
import org.springframework.core.env.Environment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DatasetStagingTaskletTest {

    @Mock
    StepContribution stepContribution;

    @Mock
    ChunkContext chunkContext;

    @Mock
    private PipelineConf pipelineConf;

    @Mock
    private Configuration configuration;

    @Mock
    private Environment env;
    @Mock
    private SparkConf sparkConf;

    @InjectMocks
    DatasetStagingTasklet  datasetStagingTasklet;

    @Mock
    private DatasetConf datasetConf;
    @Before
    public void setup(){
        Whitebox.setInternalState(datasetStagingTasklet,"driverJar","driverjar");
        Whitebox.setInternalState(datasetStagingTasklet,"mainClass","driverjar");
        Whitebox.setInternalState(datasetStagingTasklet,"hdfsUser","etl");
        Whitebox.setInternalState(datasetStagingTasklet, "jobStagingDir", System.getProperty("java.io.tmpdir"));
        when(datasetConf.getLocalPath()).thenReturn("input");
        when(datasetConf.getRemotePath()).thenReturn("output");

    }

    @Test
    public void createSparkSubmitOption() throws Exception {
        when(pipelineConf.getStagingPath()).thenReturn("test");
        JobExecution jobExecution = new JobExecution(1l);

        when(env.getProperty("server.port")).thenReturn("8080");
        when(env.getProperty("etl.host")).thenReturn("localhost");
        when(env.getProperty("staging.dataset.remotePath")).thenReturn("c://datasets");
        StepExecution stepExecution = new StepExecution("1",jobExecution);
        StepContext stepContext = new StepContext(stepExecution);
        when(chunkContext.getStepContext()).thenReturn(stepContext);
        SparkSubmitOption result =  datasetStagingTasklet.createSparkSubmitOption(stepContribution, chunkContext);

        assertThat(result).isNotNull();
        assertThat(result.getDriverJar()).isEqualTo("driverjar");
    }



}