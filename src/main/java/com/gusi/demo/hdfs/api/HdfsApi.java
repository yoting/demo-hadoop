package com.gusi.demo.hdfs.api;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * HdfsApi <br>
 *
 * @author yydeng
 * @since 2020/6/3
 */
public class HdfsApi {
	public static void main(String[] args) throws Exception {
		FileSystem fileSystem = new HdfsApi().getFileSystem();
		System.out.println(fileSystem.toString());

		// 创建文件夹
		boolean mkdirs = fileSystem.mkdirs(new Path("/hello/world"));
		System.out.println(mkdirs);

		// 创建文件并且写入数据
		FSDataOutputStream outputStream = fileSystem.create(new Path("/hello/world/test2.txt"));
		outputStream.writeUTF("hello test hdfs.");
		outputStream.close();

		// 打开文件并且读取内容
		FSDataInputStream inputStream = fileSystem.open(new Path("/hello/world/test2.txt"));
		String s = inputStream.readUTF();
		System.out.println(s);
		inputStream.close();

		// 上传文件
		fileSystem.copyFromLocalFile(new Path("file:///d:\\tmp\\test.txt"), new Path("/hello/world"));

		// 下载文件
		fileSystem.copyToLocalFile(new Path("/hello/world/test.txt"), new Path("file:///d:\\tmp\\test09.txt"));

		fileSystem.close();
	}

	// 获取文件系统
	public FileSystem getFileSystem() throws URISyntaxException, IOException {
		Configuration configuration = new Configuration();
		configuration.set("fs.defaultFS", "hdfs://10.170.3.190:8020");
		FileSystem fileSystem = FileSystem.newInstance(configuration);

		return fileSystem;
	}
}
