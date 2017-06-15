package com.xz.bigdata.mapruduce.wordcount;

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
		Configuration configuration = new Configuration();
		FileSystem fileSystem = FileSystem.get(configuration);
		Job job = Job.getInstance(configuration);
		job.setJarByClass(WordCountJob.class);
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
		FileInputFormat.setInputPaths(job, new Path("e:/input"));
		Path outputPath = new Path("e:/output");
		if (fileSystem.exists(outputPath)) {
			fileSystem.delete(outputPath, true);
		}
		FileOutputFormat.setOutputPath(job, outputPath);
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
	
}
