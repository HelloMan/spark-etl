package etl.spark.pipeline.dq;

import etl.api.job.pipeline.PipelineStep;
import etl.api.pipeline.DatasetRef;
import etl.api.pipeline.Map;
import etl.spark.TestPipelineApplication;
import etl.spark.pipeline.core.ExecutionStatus;
import etl.spark.pipeline.core.JobExecution;
import etl.spark.pipeline.core.StepExecution;
import etl.spark.pipeline.core.TransformDatasetService;
import etl.spark.pipeline.transform.TransformUtil;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Optional;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = TestPipelineApplication.class)
public class DataQualityStepListenerTest {



    @Autowired
    private DataQualityStepListener dataQualityStepListener;

    @Autowired
    private JavaSparkContext javaSparkContext;

    @Autowired
    private SparkSession sparkSession;

    private  StepExecution stepExecution;

    @MockBean
    private TransformDatasetService transformDatasetService;

    @Before
    public void init(){

        JobExecution jobExecution = new JobExecution(1l, TransformUtil.buildPipeline());
        PipelineStep pipelineStep = PipelineStep.builder()
                .name("step")
                .transform(
                        Map.builder().name("map")
                                .input(DatasetRef.builder().name("input").build())
                                .output(DatasetRef.builder().name("output").build())
                                .build()
                ).build();
        stepExecution = new StepExecution(jobExecution,pipelineStep);
    }
    @Test
    public void testBeforeStep() throws Exception {
        dataQualityStepListener.beforeStep(stepExecution);
    }

    @Test
    public void testAfterStep() throws Exception {
        stepExecution.setExecutionStatus(ExecutionStatus.COMPLETED);
        Mockito.when(transformDatasetService.getDataset(Mockito.isA(StepExecution.class), Mockito.isA(DatasetRef.class))).thenReturn(Optional.empty());
        dataQualityStepListener.afterStep(stepExecution);

        Mockito.verify(transformDatasetService).getDataset(Mockito.isA(StepExecution.class), Mockito.isA(DatasetRef.class));


    }
}