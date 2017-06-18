/**
 * Copyright © 1998-2017, Glodon Inc. All Rights Reserved.
 */
package com.xz.bigdata.mapreduce.flowcountsort;

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
		job.setMapOutputKeyClass(Flow.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Flow.class);
		FileInputFormat.setInputPaths(job, new Path("e:/output"));
		Path outputPath = new Path("e:/output1");
		if (fileSystem.exists(outputPath)) {
			fileSystem.delete(outputPath, true);
		}
		FileOutputFormat.setOutputPath(job, outputPath);

		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}
