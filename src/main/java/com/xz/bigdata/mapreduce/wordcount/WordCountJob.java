package com.xz.bigdata.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountJob {

	public static void main(String[] args) throws Exception {
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		Configuration configuration = new Configuration();
		/*
		 * configuration.set("mapreduce.framework.name", "yarn");
		 * configuration.set("yarn.resourcemanager.hostname", "study1");
		 * configuration.set("fs.defaultFS", "hdfs://study1:9000/");
		 */
		configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		FileSystem fileSystem = FileSystem.get(configuration);
		Job job = Job.getInstance(configuration);

		// job.setJarByClass(WordCountJob.class);
		job.setJar("C:\\Users\\Administrator\\Desktop\\wc.jar");

		job.setMapperClass(WordCountMapper.class);
		job.setCombinerClass(WordCountCombiner.class);
		job.setReducerClass(WordCountReduce.class);
		job.setInputFormatClass(CombineTextInputFormat.class);
		CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);
		CombineTextInputFormat.setMinInputSplitSize(job, 2097152);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(job, new Path("/wordcount/input"));
		Path outputPath = new Path("/wordcount/output");
		if (fileSystem.exists(outputPath)) {
			fileSystem.delete(outputPath, true);
		}
		FileOutputFormat.setOutputPath(job, outputPath);
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}

}
