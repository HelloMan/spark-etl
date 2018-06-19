package etl.server.service.staging.impl.job;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * properties for hold the data set configuration
 */
@ConfigurationProperties("staging.dataset")
@Getter
@Setter
public class DatasetConf {

    /**
     * the input source of dataset
     */
    private String localPath;

    /**
     * a root directory in hdfs that used to store datasets
     */
    private String remotePath;

}
