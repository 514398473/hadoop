package com.xz.bigdata.mapreduce.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JoinJob {

	public static void main(String[] args) throws Exception {
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		Configuration configuration = new Configuration();
		configuration.set("mapred.textoutputformat.separator", "\t");
		configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		FileSystem fileSystem = FileSystem.get(configuration);
		Job job = Job.getInstance(configuration);
		// job.setJarByClass(FlowJob.class);
		job.setJar("C:\\Users\\Administrator\\Desktop\\wc.jar");
		job.setMapperClass(JoinMapper.class);
		job.setReducerClass(JoinReduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Join.class);
		job.setOutputKeyClass(Join.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.setInputPaths(job, new Path("/join/input"));
		Path outputPath = new Path("/join/output");
		if (fileSystem.exists(outputPath)) {
			fileSystem.delete(outputPath, true);
		}
		FileOutputFormat.setOutputPath(job, outputPath);

		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}
