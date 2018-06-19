package etl.spark.staging.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component
public class StagingConfig {

    public static final String DATASET_PATH = "dataset.path";
    @Autowired
    private Environment environment;

    public String getDatasetRootPath() {
        return environment.getProperty(DATASET_PATH);
    }
}
