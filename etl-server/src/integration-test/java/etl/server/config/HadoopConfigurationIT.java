package etl.server.config;

import etl.common.hdfs.FileSystemTemplate;
import etl.server.IgnisApplication;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;
@RunWith(SpringRunner.class)
@AutoConfigureMockMvc
@SpringBootTest(classes = IgnisApplication.class)
public class HadoopConfigurationIT {

    @Autowired
    private FileSystemTemplate fileSystemTemplate;

    @Test
    public void testCopyFileToHdfs() throws Exception {
        if (!fileSystemTemplate.exists(new Path("/tests"))) {
            fileSystemTemplate.execute(fileSystem -> fileSystem.mkdirs(new Path("/tests")));
        }

        fileSystemTemplate.copy(new File("C:\\work\\etl\\pom.xml"), new Path("/"), false);
        assertThat(fileSystemTemplate.exists(new Path("/tests"))).isTrue();
    }
    public void testCreatePath() throws Exception {
        Path path = new Path("/tests");
        if (fileSystemTemplate.exists(path)) {
            fileSystemTemplate.execute(fileSystem -> fileSystem.delete(path, true));

        }
        fileSystemTemplate.execute(fileSystem -> fileSystem.mkdirs(path));
        assertThat(fileSystemTemplate.exists(path)).isTrue();
        fileSystemTemplate.execute(fileSystem -> fileSystem.delete(path, true));
        assertThat(fileSystemTemplate.exists(path)).isFalse();
    }
}
