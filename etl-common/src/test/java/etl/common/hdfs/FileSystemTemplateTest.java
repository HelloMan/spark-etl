package etl.common.hdfs;

import javaslang.control.Try;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class FileSystemTemplateTest {
	
	private static final String FILE_NOT_EXISTS = "fileNotExists";
	private static final String TEMP_FOLDER = "target/unittest_tmp_folder";
	private static final String TEMP_FILE = TEMP_FOLDER + "/test.txt";

	@Mock
    UserGroupInformation ugi;

    FileSystemTemplate fileSystemTemplate;
    @Before
    public void setup() {
        fileSystemTemplate =  FileSystemTemplate.builder().hdfsUser("hdfs")
                .configuration(new Configuration())
                .build();
    }

	@After
	public void teardown() {
		try {
			fileSystemTemplate.delete(TEMP_FOLDER);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

    @Test
    public void testExecute() throws Exception {
        boolean result = fileSystemTemplate.execute(fileSystem -> true);
        assertThat(result).isTrue();
    }

	@Test
	public void testMakeDir() throws Exception {
		boolean result = fileSystemTemplate.mkdirIfNotExists(new Path(TEMP_FOLDER));
		assertThat(result).isTrue();
		result = fileSystemTemplate.mkdir(new Path(TEMP_FOLDER));
		assertThat(result).isTrue();
		result = fileSystemTemplate.mkdirIfNotExists(new Path(TEMP_FOLDER));
		assertThat(result).isFalse();
		fileSystemTemplate.changePermission(new Path(TEMP_FOLDER), FsPermission.getDefault());
	}

	@Test
	public void testCopy() throws Exception {
		InputStream stream = new ByteArrayInputStream("test_string".getBytes(StandardCharsets.UTF_8.name()));
		boolean result = fileSystemTemplate.copy(stream, TEMP_FILE);
		assertThat(result).isTrue();
		result = fileSystemTemplate.deleteOnExit(TEMP_FILE);
		assertThat(result).isTrue();
	}

    @Test
    public void testBuilder() throws Exception {
        assertThat(fileSystemTemplate).isNotNull();
    }

    @Test(expected = IOException.class)
    public void testExecute_ThrowException() throws Exception {
        Try.CheckedFunction<FileSystem, Boolean> checkedFunction = (fs) -> {
            throw new IOException("E");
        };
        fileSystemTemplate.execute(checkedFunction);

    }

    @Test(expected = IOException.class)
    public void testExecute_ThrowRuntimeException() throws Exception {
        Try.CheckedFunction<FileSystem, Boolean> checkedFunction = (fs) -> {
            throw new RuntimeException("E");
        };
        fileSystemTemplate.execute(checkedFunction);
    }

    @Test
    public void testMapTry_ThrowException() throws Exception {
        Try.CheckedFunction<FileSystem, Boolean> checkedFunction = (fs) -> {
            throw new RuntimeException("E");
        };
        assertThat(fileSystemTemplate.map(checkedFunction).isFailure()).isTrue();

    }

    @Test
    public void testMapTry() throws Exception {
        Try.CheckedFunction<FileSystem, Boolean> checkedFunction = (fs) -> true;
        assertThat(fileSystemTemplate.map(checkedFunction).isSuccess()).isTrue();

    }

	@Test(expected = FileNotFoundException.class)
	public void open_fail() throws IOException {
		fileSystemTemplate.open(new Path(FILE_NOT_EXISTS));
	}

	@Test
	public void exists() throws IOException {
		boolean res = fileSystemTemplate.exists(new Path(FILE_NOT_EXISTS));
		assertThat(res).isFalse();
	}

	@Test(expected = IOException.class)
	public void copy_file_to_path() throws IOException {
		boolean res = fileSystemTemplate.copy(new File(FILE_NOT_EXISTS), new Path(FILE_NOT_EXISTS), false);
		assertThat(res).isFalse();
	}

	@Test(expected = IOException.class)
	public void copy_file_to_pathStr() throws IOException {
		boolean res = fileSystemTemplate.copy(new File(FILE_NOT_EXISTS), FILE_NOT_EXISTS, false);
		assertThat(res).isFalse();
	}

	@Test(expected = IOException.class)
	public void copy_in_to_path() throws IOException {
		boolean res = fileSystemTemplate.copy(new FileInputStream(FILE_NOT_EXISTS), new Path(FILE_NOT_EXISTS));
		assertThat(res).isFalse();
	}

	@Test(expected = IOException.class)
	public void copy_in_to_pathStr() throws IOException {
		boolean res = fileSystemTemplate.copy(new FileInputStream(FILE_NOT_EXISTS), FILE_NOT_EXISTS);
		assertThat(res).isFalse();
	}

	@Test
	public void delete() throws IOException {
		boolean res = fileSystemTemplate.delete(FILE_NOT_EXISTS);
		assertThat(res).isFalse();
	}

	@Test
	public void delete_recursive() throws IOException {
		boolean res = fileSystemTemplate.delete(new Path(FILE_NOT_EXISTS), true);
		assertThat(res).isFalse();
	}

	@Test
	public void deleteOnExit_pathStr() throws IOException {
		boolean res = fileSystemTemplate.deleteOnExit(FILE_NOT_EXISTS);
		assertThat(res).isFalse();
	}

	@Test
	public void deleteOnExit_path() throws IOException {
		boolean res = fileSystemTemplate.deleteOnExit(new Path(FILE_NOT_EXISTS));
		assertThat(res).isFalse();
	}

}