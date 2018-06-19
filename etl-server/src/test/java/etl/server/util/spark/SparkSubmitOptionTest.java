package etl.server.util.spark;

import etl.api.job.JobType;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.junit.Test;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

/**
 * Created by Jack Yang on 9/7/2017.
 */
public class SparkSubmitOptionTest {
    @Test
    public void getterAndSetterCorrectness() throws Exception {
        SparkSubmitOption sparkSubmitOption = SparkSubmitOption.builder()
                .clazz("class")
                .jobType(JobType.STAGING_DATASET)
                .driverJar("driverJar")
                .hdfsUser("etl")
                .jobStagingBaseDir("stagingPath")
                .jobName("jobName")
                .sparkConf(new SparkConf())
                .hadoopConf(new Configuration())
                .build();
        assertThat(sparkSubmitOption.toString()).isNotNull();
    }

    @Test
    public void equalsAndHashCodeContract() {
        SparkSubmitOption sparkSubmitOption = SparkSubmitOption.builder()
                .clazz("class")
                .driverJar("driverJar")
                .jobType(JobType.STAGING_DATASET)
                .jobStagingBaseDir("stagingPath")
                .hdfsUser("etl")
                .jobName("jobName")
                .sparkConf(new SparkConf())
                .hadoopConf(new Configuration())
                .build();

        SparkSubmitOption sparkSubmitOption2 = SparkSubmitOption.builder()
                .clazz("")
                .driverJar("")
                .jobType(JobType.STAGING_DATASET)
                .hdfsUser("")
                .jobName("")
                .jobStagingBaseDir("stagingPath")
                .sparkConf(new SparkConf())
                .hadoopConf(new Configuration())
                .build();
        assertThat(sparkSubmitOption2).isNotEqualTo(sparkSubmitOption);
    }
}
