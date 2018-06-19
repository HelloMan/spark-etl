package etl.server.service.common;

import etl.server.util.spark.SparkSubmitOption;
import etl.server.util.spark.SparkYarnSubmitter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.spark.SparkConf;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import scala.Tuple2;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class StoppableSparkJobTaskletTest {
    @Mock
    StepContribution stepContribution;

    @Mock
    ChunkContext chunkContext;

    @Mock
    YarnClient yarnClient;

    @Mock
    YarnApplicationIdHolder applicationIdHolder;
    @InjectMocks
    MockStoppableSparkJobTasklet tasklet;


    private static class  MockStoppableSparkJobTasklet extends StoppableSparkJobTasklet {
        @Override
        protected SparkSubmitOption createSparkSubmitOption(StepContribution contribution, ChunkContext chunkContext)   {
            return SparkSubmitOption.builder()
                    .driverJar("driverJar")
                    .clazz("clazz")
                    .arg("test")
                    .hdfsUser("etl")
                    .jobName("mockJob")
                    .sparkConf(new SparkConf())
                    .hadoopConf(new Configuration())
                    .build();
        }
    }






    @Test
    public void stop() throws Exception {
        ApplicationId applicationId = ApplicationId.newInstance(1l, 2);
        when(applicationIdHolder.getValue()).thenReturn(applicationId);
        Mockito.doNothing().when(yarnClient).killApplication(applicationId);

        tasklet.stop();
        Mockito.verify(yarnClient).killApplication(applicationId);

    }

    @Test
    public void submitAndMonitorApplication() throws Exception {

        SparkYarnSubmitter sparkYarnSubmitter = Mockito.mock(SparkYarnSubmitter.class);
        ApplicationId applicationId = ApplicationId.newInstance(1l, 2);
        when(sparkYarnSubmitter.submitApplication()).thenReturn(applicationId);
        when(sparkYarnSubmitter.monitorApplication(applicationId)).thenReturn(new Tuple2<>(YarnApplicationState.FINISHED, FinalApplicationStatus.SUCCEEDED));


        tasklet.submitAndMonitorApplication(stepContribution,sparkYarnSubmitter);

        verify(sparkYarnSubmitter).submitApplication();
        verify(sparkYarnSubmitter).monitorApplication(applicationId);

    }

}