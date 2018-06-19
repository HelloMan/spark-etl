package etl.server.service.pipeline.impl.job;

import etl.api.job.JobType;
import etl.api.job.pipeline.PipelineJobRequest;
import etl.common.json.MapperWrapper;
import etl.server.repository.ServiceRequestRepository;
import etl.server.service.common.StoppableSparkJobTasklet;
import etl.server.config.AppConfig;
import etl.api.job.JobConfig;
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

@Slf4j
public class DataQualityCheckTasklet extends StoppableSparkJobTasklet {

    @Value("${spark.drivers.dq.resource}")
    private String driverJar;

    @Value("${spark.drivers.dq.mainClass}")
    private String mainClass;

    @Value("${hadoop.user}")
    private String hdfsUser;

    @Value("${job.staging.base.dir}")
    private String jobStagingDir;

	@Value("#{jobParameters['serviceRequestId']}")
	private long serviceRequestId;

    @Autowired
    private Environment env;

    @Autowired
    private Configuration configuration;

    @Autowired
    private SparkConf sparkConf;

    @Autowired
    private ServiceRequestRepository serviceRequestRepository;

    @Override
    protected SparkSubmitOption createSparkSubmitOption(StepContribution contribution, ChunkContext chunkContext) throws IOException {
		String requestMessage = serviceRequestRepository.findOne(serviceRequestId).getRequestMessage();
        PipelineJobRequest jobRequest = MapperWrapper.MAPPER.readValue(requestMessage, PipelineJobRequest.class);
        AppConfig appConfig = new AppConfig(env);
        return SparkSubmitOption.builder()
                .clazz(mainClass)
                .jobExecutionId(serviceRequestId)
                .job(jobRequest)
                .jobType(JobType.DATA_QUALITY_CHECK)
                .jobStagingBaseDir(jobStagingDir)
                .hdfsUser(hdfsUser)
                .jobName("DataQualityCheck")
                .driverJar(driverJar)
                .hadoopConf(configuration)
                .sparkConf(sparkConf)
                .arg(requestMessage)
				.jobProp(JobConfig.IGNIS_HTTP_WEB_ADDRESS, appConfig.getHttpWebAddress())
				.jobProp(JobConfig.ZOOKEEPER_URL, env.getProperty(JobConfig.ZOOKEEPER_URL))
				.jobProp(JobConfig.HBASE_ROOTDIR, env.getProperty(JobConfig.HBASE_ROOTDIR))
				.jobProp(JobConfig.PHOENIX_SALTBUCKETS, env.getProperty(JobConfig.PHOENIX_SALTBUCKETS))
                .build();
    }



}
