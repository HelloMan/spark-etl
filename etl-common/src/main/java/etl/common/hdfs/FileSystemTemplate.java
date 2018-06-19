package etl.common.hdfs;

import javaslang.control.Try;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.PrivilegedAction;

@Slf4j
public  class FileSystemTemplate {

	private final Configuration configuration;

	private final UserGroupInformation ugi;


	@Builder
	public FileSystemTemplate(@NonNull Configuration configuration, @NonNull String hdfsUser) {
		this.configuration = configuration;
		ugi = UserGroupInformation.createRemoteUser(hdfsUser);
	}

	/**
     *
     * @param mapper
     * @param <T>
     * @return
     * @throws IOException
     */
    public  <T> T execute(Try.CheckedFunction<FileSystem,T> mapper) throws IOException {
		return  ugi.doAs((PrivilegedAction<Try<T>>) () -> {
			Try<FileSystem> fileSystem = Try.of(() -> FileSystem.get(configuration));
			return fileSystem.mapTry(mapper);
		}).getOrElseThrow(this::toIoException);

	}

	private IOException toIoException(Throwable e) {
		return e instanceof IOException ?(IOException) e :new IOException(e);
	}

	public <T> Try<T> map(Try.CheckedFunction<FileSystem,T> mapper){
		return ugi.doAs((PrivilegedAction<Try<T>>) () -> {
			Try<FileSystem> fileSystems = Try.of(() -> FileSystem.get(configuration));
			return fileSystems.mapTry(mapper);
		});

	}

	/**
	 * get file content as input stream by a given hdfs path
	 * @param path
	 * @return InputStream
	 */
	public InputStream open(Path path) throws IOException {
		return execute(fileSystem -> fileSystem.open(path));
	}

	/**
	 * return true if path exists in remote hdfs file system
	 * @param path A path in hdfs file system
	 * @return true if path exists
	 * @throws IOException
	 */
	public boolean exists(Path path) throws IOException {
		return execute(fileSystem -> fileSystem.exists(path));
	}

	/**
	 * Create directory.
	 */
	public boolean mkdir(Path path) throws IOException {
		return execute(fileSystem -> fileSystem.mkdirs(path));
	}

	/**
	 * Create directory if it not exists, otherwise do nothing.
	 */
	public boolean mkdirIfNotExists(Path path) throws IOException {
		if (!exists(path)) {
			log.info("Creating HDFS path: {}", path);
			return execute(fileSystem -> fileSystem.mkdirs(path));
		}
		return false;
	}

	/**
	 * Change permission of path if it exists, similar with chmod in linux.
	 */
	public void changePermission(Path path, FsPermission permission) throws IOException {
		if (exists(path)) {
			execute(fileSystem -> {
				fileSystem.setPermission(path, permission);
				return null;
			});
		}
	}

	/**
	 * Return True if successfully copy files from local file system to remote hadoop hdfs file system
	 * @param src Local file
	 * @param dst Remote file path
	 * @param deleteSource Whether to remove source file
	 * @return True if file copy successfully
	 * @throws IOException
	 */
	public boolean copy(File src, Path dst, boolean deleteSource) throws IOException {
		return execute(fileSystem -> FileUtil.copy(src, fileSystem, dst, deleteSource, configuration));
	}

	public boolean copy(File src, String dst, boolean deleteSource) throws IOException {
		return execute(fileSystem -> FileUtil.copy(src, fileSystem, new Path(dst), deleteSource, configuration));
	}

	public boolean copy(InputStream src, Path dst) throws IOException {
		return execute(fileSystem -> {
			OutputStream out = fileSystem.create(dst);
			IOUtils.copyBytes(src, out, configuration);
			return true;
		});

	}
	/**
	 * copy files from web/restful to remote hadoop hdfs file system
	 * @param srcIn InputStream file
	 * @param dst Remote file path
	 * @return True if file copy successfully
	 * @throws IOException
	 */
	public boolean copy(InputStream srcIn, String dst) throws IOException {
		return copy(srcIn, new Path(dst));
	}

	/**
	 * Delete a file or folder and all its sub contents recursively.
	 * @param dst
	 * @return
	 * @throws IOException
	 */
	public boolean delete(String dst) throws IOException {
		log.info("deleting HDFS file {}", dst);
		return delete(new Path(dst), true);
	}

	public boolean deleteOnExit(Path dst) throws IOException {
		log.info("deleting HDFS file {}", dst);
		if (exists(dst)) {
			return delete(dst,true);
		}
		return false;
	}
	public boolean deleteOnExit(String dst) throws IOException {
		return deleteOnExit(new Path(dst));
	}

	/**
	 * * Delete a file.
	 * @param path
	 * @param recursive remove file or path recursive
	 * @return true -success ,false - fail
	 * @throws IOException
	 */
	public boolean delete(Path path,boolean recursive) throws IOException {
		return execute(fileSystem -> fileSystem.delete(path,recursive));
	}


}
