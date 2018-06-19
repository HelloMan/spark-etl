package etl.server.service.pipeline.impl.job;

import etl.api.job.pipeline.PipelineJobRequest;
import etl.common.json.MapperWrapper;
import etl.server.domain.entity.ServiceRequest;
import etl.server.repository.ServiceRequestRepository;
import etl.server.util.spark.SparkSubmitOption;
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
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DataQualityCheckTaskletTest {
    @Mock
    StepContribution stepContribution;

    @Mock
    ChunkContext chunkContext;

    @Mock
    private PipelineConf pipelineConf;

    @Mock
    private Configuration configuration;

    @Mock
    private SparkConf sparkConf;

    @Mock
    private Environment env;
    @Mock
    private ServiceRequestRepository serviceRequestRepository;
    @InjectMocks
    DataQualityCheckTasklet dataQualityCheckTasklet;
    @Before
    public void setup(){
        Whitebox.setInternalState(dataQualityCheckTasklet,"driverJar","driverjar");
        Whitebox.setInternalState(dataQualityCheckTasklet,"mainClass","driverjar");
        Whitebox.setInternalState(dataQualityCheckTasklet,"hdfsUser","etl");
        Whitebox.setInternalState(dataQualityCheckTasklet,"jobStagingDir",System.getProperty("java.io.tmpdir"));


    }

    @Test
    public void createSparkSubmitOption() throws Exception {
        ServiceRequest serviceRequest = new ServiceRequest();
		PipelineJobRequest job = PipelineJobRequest.builder().name("job").build();
        serviceRequest.setRequestMessage(MapperWrapper.MAPPER.writeValueAsString(job));

        when(env.getProperty("server.port")).thenReturn("8080");
        when(env.getProperty("etl.host")).thenReturn("localhost");
        when(serviceRequestRepository.findOne(anyLong())).thenReturn(serviceRequest);
        when(pipelineConf.getStagingPath()).thenReturn("test");
        JobExecution jobExecution = new JobExecution(1l);

        StepExecution stepExecution = new StepExecution("1",jobExecution);
        StepContext stepContext = new StepContext(stepExecution);
        when(chunkContext.getStepContext()).thenReturn(stepContext);
        SparkSubmitOption result =  dataQualityCheckTasklet.createSparkSubmitOption(stepContribution, chunkContext);

        assertThat(result).isNotNull();
        assertThat(result.getDriverJar()).isEqualTo("driverjar");
    }

}