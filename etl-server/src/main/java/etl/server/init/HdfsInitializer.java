package etl.server.init;

import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import etl.common.hdfs.FileSystemTemplate;
import etl.common.annotation.ExcludeFromTest;
import com.machinezoo.noexception.Exceptions;
import lombok.extern.slf4j.Slf4j;
import one.util.streamex.StreamEx;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.function.Consumer;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

@Component
@Slf4j
@ExcludeFromTest
public class HdfsInitializer implements ApplicationRunner {
    private static final String SPARK_LOCALIZED_LIB_DIR = "__spark_libs__";

    @Value("${hadoop.user}")
    private String hadoopUser;

    @Autowired
    private FileSystemTemplate fileSystemTemplate;

    @Autowired
    private SparkConf sparkConf;

    @Value("${spark.jars.dir}")
    private String sparkJarsDir;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        String sparkLogPath = String.format("/user/%s/app-logs/spark", hadoopUser);
        String datasetSchemaPath = String.format("/user/%s/datasets/schema", hadoopUser);

        StreamEx.of(sparkLogPath, datasetSchemaPath).forEach(createPath());


        uploadFiles();
    }

    private Consumer<String> createPath() {
        return Exceptions.sneak().consumer(path -> {
            Preconditions.checkArgument(StringUtils.isNotBlank(path), "Cannot init hdfs path with empty value");
            fileSystemTemplate.mkdirIfNotExists(new Path(path));
        });
    }



    private void uploadFiles() throws IOException {
        uploadSparkJars();
    }

    /**
     * Upload spark jars under SPARK_HOME/jars to HDFS.
     */
    private void uploadSparkJars() throws IOException {
        Path sparkJarsRemoteDir = new Path(sparkConf.get("spark.yarn.archive"));
        if (!fileSystemTemplate.exists(sparkJarsRemoteDir)) {
			log.info("Start to spark jar archive to HDFS.");
            File jarsArchive = File.createTempFile(SPARK_LOCALIZED_LIB_DIR, ".zip");
            try (ZipOutputStream jarsStream = new ZipOutputStream(new FileOutputStream(jarsArchive))) {
                jarsStream.setLevel(0);
                File[] jarFiles = new File(sparkJarsDir).listFiles(pathname -> pathname.isFile() && pathname.getName().toLowerCase().endsWith(".jar") && pathname.canRead());
                Consumer<File> addEntry = Exceptions.sneak().consumer(jarFile -> {
                            jarsStream.putNextEntry(new ZipEntry(jarFile.getName()));
                            Files.copy(jarFile, jarsStream);
                            jarsStream.closeEntry();
                        }
                );
                if (jarFiles != null) {
                    Arrays.stream(jarFiles).forEach(addEntry);
                }
            }
            fileSystemTemplate.copy(jarsArchive, sparkJarsRemoteDir, true);
			log.info("Finished to upload file to HDFS.");
        }

    }

}