package etl.server.config;

import com.google.common.annotations.VisibleForTesting;
import etl.common.annotation.ExcludeFromTest;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

@Configuration
@ExcludeFromTest
public class SparkBean {

    @Value("${spark.conf.dir}")
    private String sparkConfDir;

    @VisibleForTesting
    void setSparkConfDir(String sparkConfDir) {
        this.sparkConfDir = sparkConfDir;
    }

    @Bean
    public SparkConf sparkconf() throws IOException {
        String sparkConfProp = StringUtils.appendIfMissing(sparkConfDir, File.separator) + "spark-defaults.conf";
        Properties properties = PropertiesLoaderUtils.loadProperties(new FileSystemResource(new File(sparkConfProp)));
        SparkConf result = new SparkConf();
        properties.stringPropertyNames().forEach(key -> result.set(key, properties.getProperty(key)));
        return result;
    }
 

}
