/**
 * Copyright © 1998-2017, Glodon Inc. All Rights Reserved.
 */
package com.xz.bigdata.mapreduce.flowcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 此处填写类简介
 * <p>
 * 此处填写类说明
 * </p>
 * 
 * @author xuz-d
 * @since jdk1.6 2017年6月14日
 */

public class FlowJob {

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		FileSystem fileSystem = FileSystem.get(configuration);
		Job job = Job.getInstance(configuration);
		job.setJarByClass(FlowJob.class);
		job.setMapperClass(FlowCountMapper.class);
		job.setReducerClass(FlowCountReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Flow.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Flow.class);
		FileInputFormat.setInputPaths(job, new Path("E:/input"));
		Path outputPath = new Path("E:/output");
		if (fileSystem.exists(outputPath)) {
			fileSystem.delete(outputPath, true);
		}
		FileOutputFormat.setOutputPath(job, outputPath);

		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}
