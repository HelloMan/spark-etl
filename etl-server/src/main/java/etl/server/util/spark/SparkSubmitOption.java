package etl.server.util.spark;

import etl.api.job.JobConfig;
import etl.api.job.JobType;
import etl.common.json.MapperWrapper;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.Singular;
import lombok.ToString;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Builder
@Getter
@EqualsAndHashCode(exclude = {"sparkConf","args","hadoopConf"})
@ToString
public class SparkSubmitOption {

    private static final String JOB_STAGING_DIR = ".etlStaging";
    private static final String JOB_JSON_FILE = "job.json";
    private static final String JOB_PROPERTIES_FILE = "job.properties";
    @NonNull
    private String jobStagingBaseDir;

    @NonNull
    private long jobExecutionId;

    private Object job;

    @NonNull
    private final String jobName;

    @NonNull
    private final String driverJar;
    @NonNull
    private final JobType jobType;

    @NonNull
    private final String clazz;

    @Singular
    private final List<String> args;
    @NonNull
    private final SparkConf sparkConf;
    @NonNull
    private final Configuration hadoopConf;
    @NonNull
    private final String hdfsUser;
    @Singular
    private Map<String,String> jobProps;


    public String getJobStagingDir(){
        return String.join(File.separator, getJobStagingBaseDir(), JOB_STAGING_DIR, String.valueOf(getJobExecutionId()));
    }
    /**
     * create and store file into job staging directory
     * @return
     */
    public List<String> getFiles(){
        if (getJob() == null || getJobProps() == null || getJobProps().isEmpty()) {
            return Collections.emptyList();
        }
        final List<String> result = new ArrayList<>();
        try {
			File jobStagingDirFile = new File(getJobStagingDir());
			if (!jobStagingDirFile.exists()) {
				FileUtils.forceMkdir(jobStagingDirFile);
			}
			result.add(createJobFile());
            result.add(createJobPropertyFile());
        } catch (IOException e) {
            throw new SparkSubmitException(e);
        }
        return result;

    }

    private String createJobFile() throws IOException {
        String result = String.join(File.separator, getJobStagingDir(), JOB_JSON_FILE);
        MapperWrapper.MAPPER.writeValue(new File(result), getJob());
        return result;
    }

    private String createJobPropertyFile() throws IOException {
        String result =String.join(File.separator, getJobStagingDir(), JOB_PROPERTIES_FILE);
        Properties properties = new Properties();
        properties.setProperty(JobConfig.JOB_ID, String.valueOf(getJobExecutionId()));
        properties.setProperty(JobConfig.JOB_TYPE, getJobType().name());
        properties.putAll(getJobProps());
        try(FileOutputStream outputStream = new FileOutputStream(result)) {
            properties.store(outputStream,"job properties");
        }
        return result;
    }
}
