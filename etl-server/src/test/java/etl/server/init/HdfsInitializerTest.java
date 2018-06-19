package etl.server.init;

import etl.common.hdfs.FileSystemTemplate;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.spark.SparkConf;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.internal.util.reflection.Whitebox;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.boot.ApplicationArguments;
import org.springframework.core.env.Environment;

import java.io.File;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class HdfsInitializerTest {

	@InjectMocks
    HdfsInitializer hdfsInitializer;

	@Mock
	FileSystemTemplate fileSystemTemplate;


	@Mock
	private SparkConf sparkConf;

	@Mock
	private Environment environment;

	@Before
	public void setup() {
		Whitebox.setInternalState(hdfsInitializer, "hadoopUser", "etl");

		String sparkJarsDir = getClass().getClassLoader().getResource("spark/jars").getPath();
		Whitebox.setInternalState(hdfsInitializer, "sparkJarsDir", sparkJarsDir);
	}

	@Test
	public void testRun() throws Exception {
		when(sparkConf.get("spark.yarn.archive")).thenReturn("abc");
		//arrange
		when(fileSystemTemplate.exists(any(org.apache.hadoop.fs.Path.class))).thenReturn(false);
		when(fileSystemTemplate.copy(any(File.class), any(org.apache.hadoop.fs.Path.class), anyBoolean())).thenReturn(true);
		when(fileSystemTemplate.mkdirIfNotExists(any(org.apache.hadoop.fs.Path.class))).thenReturn(true);
		doNothing().when(fileSystemTemplate).changePermission(any(org.apache.hadoop.fs.Path.class), any(FsPermission.class));

		//act
		hdfsInitializer.run(mock(ApplicationArguments.class));

		//assert
		verify(fileSystemTemplate, atLeastOnce()).mkdirIfNotExists(any(org.apache.hadoop.fs.Path.class));
	}
}