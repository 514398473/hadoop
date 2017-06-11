package com.xz.bigdata.hdfs.test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;

/**
 * 测试hdfs客户端
 * 
 * @author Administrator
 *
 */
public class TestHDFSClient {

	private FileSystem fileSystem = null;

	@Before
	public void initFileSystem() {
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		Configuration configuration = new Configuration();
		configuration.set("fs.defaultFS", "hdfs://study1:9000/");
		try {
			fileSystem = FileSystem.get(configuration);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void upload() {
		try {
			fileSystem.copyFromLocalFile(new Path("e:/天行vpn使用说明.txt"), new Path("/"));
			fileSystem.close();
		} catch (IllegalArgumentException | IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void download() {
		try {
			fileSystem.copyToLocalFile(new Path("/天行vpn使用说明.txt"), new Path("c:/天行vpn使用说明.txt"));
			fileSystem.close();
		} catch (IllegalArgumentException | IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void delete() {
		try {
			fileSystem.delete(new Path("/天行vpn使用说明.txt"), true);
			fileSystem.close();
		} catch (IllegalArgumentException | IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void rename() {
		try {
			fileSystem.rename(new Path("/天行vpn使用说明.txt"), new Path("/a.txt"));
		} catch (IllegalArgumentException | IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void mkdirs() {
		try {
			fileSystem.mkdirs(new Path("/b/b/b"));
		} catch (IllegalArgumentException | IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void listFiles() {
		try {
			RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("/"), true);
			while (listFiles.hasNext()) {
				LocatedFileStatus locatedFileStatus = (LocatedFileStatus) listFiles.next();
				System.out.println(locatedFileStatus.getBlockSize());
				System.out.println(locatedFileStatus.getGroup());
				System.out.println(locatedFileStatus.getLen());
				System.out.println(locatedFileStatus.getOwner());
				System.out.println(locatedFileStatus.getPath());
				System.out.println(locatedFileStatus.getPermission());
				System.out.println(locatedFileStatus.getReplication());
				System.out.println(Arrays.asList(locatedFileStatus.getBlockLocations()).toString());
				System.err.println("----------------------------------------------------");
			}
		} catch (IllegalArgumentException | IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void listStatus() {
		try {
			FileStatus[] listStatus = fileSystem.listStatus(new Path("/"));
			for (FileStatus fileStatus : listStatus) {
				System.out.println(fileStatus.getBlockSize());
				System.out.println(fileStatus.getGroup());
				System.out.println(fileStatus.getLen());
				System.out.println(fileStatus.getOwner());
				System.out.println(fileStatus.getPath().getName());
				System.out.println(fileStatus.getPermission());
				System.out.println(fileStatus.getReplication());
				System.err.println("----------------------------------------------------");
			}
		} catch (IllegalArgumentException | IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testStreamUpload() {
		try {
			FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/test.txt"));
			FileInputStream fileInputStream = new FileInputStream("e:/log_network.txt");
			IOUtils.copy(fileInputStream, fsDataOutputStream);
			fileSystem.close();
		} catch (IllegalArgumentException | IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testStreamDownload() {
		try {
			FSDataInputStream fsDataInputStream = fileSystem.open(new Path("/test.txt"));
			FileOutputStream fileOutputStream = new FileOutputStream("e:/1.txt");
			IOUtils.copy(fsDataInputStream, fileOutputStream);
			fileSystem.close();
		} catch (IllegalArgumentException | IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testRandomAccess() {
		try {
			FSDataInputStream fsDataInputStream = fileSystem.open(new Path("/test.txt"));
			FileOutputStream fileOutputStream = new FileOutputStream("e:/1.txt");
			IOUtils.copyLarge(fsDataInputStream, fileOutputStream, 2024, Long.MAX_VALUE);
			fileSystem.close();
		} catch (IllegalArgumentException | IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testCat() {
		try {
			FSDataInputStream fsDataInputStream = fileSystem.open(new Path("/test.txt"));
			IOUtils.copy(fsDataInputStream, System.out);
			fileSystem.close();
		} catch (IllegalArgumentException | IOException e) {
			e.printStackTrace();
		}
	}

}
