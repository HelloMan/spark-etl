package etl.server.config;

import etl.common.hdfs.FileSystemTemplate;
import etl.common.annotation.ExcludeFromTest;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;

import java.io.IOException;

/**
 * A configuration bean for hadoop configuration
 */
@org.springframework.context.annotation.Configuration
@ExcludeFromTest
public class HadoopBean {



    @Value("${hadoop.user}")
    private String hadoopUser;

    @Value("${hadoop.conf.dir}")
    private String hadoopConfDir;
    @Bean
    public Configuration hdfsConfiguration() throws IOException {
        org.apache.hadoop.conf.Configuration result = new org.apache.hadoop.conf.Configuration();
        result.addResource(new Path(StringUtils.appendIfMissing(hadoopConfDir, Path.SEPARATOR) + "core-site.xml"));
        result.addResource(new Path(StringUtils.appendIfMissing(hadoopConfDir, Path.SEPARATOR) + "hdfs-site.xml"));
        result.addResource(new Path(StringUtils.appendIfMissing(hadoopConfDir, Path.SEPARATOR) + "yarn-site.xml"));
        return result;
    }

    @Bean
    public FileSystemTemplate fileSystemTemplate(@Autowired Configuration configuration) throws IOException {
        return FileSystemTemplate.builder()
                .hdfsUser(hadoopUser)
                .configuration(configuration)
                .build();
    }


    @Bean
    @Lazy(value = true)
    public YarnClient sparkYarnClient(@Autowired Configuration configuration) throws IOException {
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(configuration);
        yarnClient.start();
        return yarnClient;
    }

}
