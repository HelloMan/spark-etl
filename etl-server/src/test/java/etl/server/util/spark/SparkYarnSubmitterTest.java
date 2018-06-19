package etl.server.util.spark;

import etl.api.job.JobType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.yarn.Client;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import scala.Tuple2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SparkYarnSubmitterTest {
    @Mock
    private Client client;
    private  SparkSubmitOption sparkSubmitOption;

    @Before
    public void setup(){
        sparkSubmitOption = SparkSubmitOption.builder()
                .clazz("class")
                .jobStagingBaseDir("/stagingPath")
                .jobExecutionId(1l)
                .jobType(JobType.DATA_QUALITY_CHECK)
                .job(new Object())
                .driverJar("driverJar")
                .hdfsUser("etl")
                .jobName("jobName")
                .sparkConf(new SparkConf())
                .hadoopConf(new Configuration())
                .build();

    }


    @Test
    public void submitApplication() throws Exception {
        ApplicationId applicationId = ApplicationId.newInstance(1l, 2);
        when(client.submitApplication()).thenReturn(applicationId);
        SparkConf sparkConf = new SparkConf();
        sparkConf.set("hadoop.user", "etl");
        when(client.sparkConf()).thenReturn(sparkConf);
        SparkYarnSubmitter sparkYarnSubmitter = new SparkYarnSubmitter(client,sparkSubmitOption);
        ApplicationId result = sparkYarnSubmitter.submitApplication();
        assertThat(result.toString()).isEqualTo(applicationId.toString());
    }

    @Test
    public void monitorApplication() throws Exception {
        ApplicationId applicationId = ApplicationId.newInstance(1l, 2);
        when(client.monitorApplication(applicationId, false, true)).thenReturn(new Tuple2(YarnApplicationState.FINISHED, FinalApplicationStatus.SUCCEEDED));
        SparkYarnSubmitter sparkYarnSubmitter = new SparkYarnSubmitter(client,sparkSubmitOption);
        Tuple2<YarnApplicationState,FinalApplicationStatus> result = sparkYarnSubmitter.monitorApplication(applicationId);
        assertThat(result._1()).isEqualTo(YarnApplicationState.FINISHED);
    }


    @Test
    public void close() throws Exception {
        doNothing().when(client).stop();
        SparkYarnSubmitter sparkYarnSubmitter = new SparkYarnSubmitter(client,sparkSubmitOption);
        sparkYarnSubmitter.close();
        verify(client).stop();

    }

}