package etl.server.service.staging.impl.job;

import etl.api.job.JobConfig;
import etl.api.job.JobType;
import etl.api.job.staging.StagingJobRequest;
import etl.common.json.MapperWrapper;
import etl.server.config.AppConfig;
import etl.server.service.common.StoppableSparkJobTasklet;
import etl.server.util.spark.SparkSubmitOption;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;

import java.io.IOException;

/**
 * dataset validate will submit a spark driver into yarn cluster
 */
@Slf4j
public class DatasetStagingTasklet extends StoppableSparkJobTasklet {

    @Value("${spark.drivers.staging.resource}")
    private String driverJar;

    @Value("${spark.drivers.staging.mainClass}")
    private String mainClass;

    @Value("${hadoop.user}")
    private String hdfsUser;

	@Value("${job.staging.base.dir}")
	private String jobStagingDir;

	@Autowired
	private Environment env;

    @Autowired
    private Configuration configuration;

    @Autowired
    private SparkConf sparkConf;

    @Autowired
    private DatasetConf datasetConf;

	@Value("#{jobParameters['serviceRequestId']}")
	private long serviceRequestId;

    @Override
    protected SparkSubmitOption createSparkSubmitOption(StepContribution contribution, ChunkContext chunkContext) throws IOException {
		final StagingJobRequest jobRequest = (StagingJobRequest) chunkContext.getStepContext()
				.getStepExecution().getJobExecution().getExecutionContext().get("jobRequest");
		AppConfig appConfig = new AppConfig(env);
		return SparkSubmitOption.builder()
				.clazz(mainClass)
				.jobExecutionId(serviceRequestId)
				.job(jobRequest)
				.jobType(JobType.STAGING_DATASET)
				.jobStagingBaseDir(jobStagingDir)
				.hdfsUser(hdfsUser)
				.jobName("StagingDataset")
				.driverJar(driverJar)
				.hadoopConf(configuration)
				.sparkConf(sparkConf)
				.arg(MapperWrapper.MAPPER.writeValueAsString(jobRequest))
				.jobProp(JobConfig.IGNIS_HTTP_WEB_ADDRESS, appConfig.getHttpWebAddress())
				.jobProp(JobConfig.STAGING_DATASET_PATH, datasetConf.getRemotePath())
				.jobProp(JobConfig.ZOOKEEPER_URL, env.getProperty(JobConfig.ZOOKEEPER_URL))
				.jobProp(JobConfig.HBASE_ROOTDIR, env.getProperty(JobConfig.HBASE_ROOTDIR))
				.jobProp(JobConfig.PHOENIX_SALTBUCKETS, env.getProperty(JobConfig.PHOENIX_SALTBUCKETS))
				.build();
	}

}
