package etl.spark.config;

import etl.api.job.JobType;
import etl.spark.core.DriverContext;
import lombok.Getter;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component
public class DefaultDriverContext implements DriverContext {
    @Value("${job.id}")
    @Getter
    private long jobExecutionId;

    @Value("${job.type}")
    private String jobType;

    @Value("${debug.mode}")
    @Getter
    private boolean debugMode;

    @Autowired
    private Environment environment;

    @Autowired
    @Getter
    private SparkSession sparkSession;

    @Override
    public JobType getJobType() {
        return JobType.valueOf(jobType);
    }

    @Override
    public String getProperty(String key) {
        return environment.getProperty(key);
    }

    @Override
    public int getInteger(String key) {
        return environment.getProperty(key, Integer.class);
    }

    @Override
    public long getLong(String key) {
        return environment.getProperty(key, Long.class);
    }

    @Override
    public boolean getBoolean(String key) {
        return environment.getProperty(key, Boolean.class);
    }


}
