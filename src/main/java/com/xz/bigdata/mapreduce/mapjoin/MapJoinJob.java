package com.xz.bigdata.mapreduce.mapjoin;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapJoinJob {

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		FileSystem fileSystem = FileSystem.get(configuration);
		Job job = Job.getInstance(configuration);
		job.setJarByClass(MapJoinJob.class);
		job.setMapperClass(MapJoinMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(0);
		job.addCacheFile(new URI("file:/E:/cache/pdts.txt"));
		FileInputFormat.setInputPaths(job, new Path("E:/input"));
		Path outpath = new Path("E:/output");
		if (fileSystem.exists(outpath)) {
			fileSystem.delete(outpath, true);
		}
		FileOutputFormat.setOutputPath(job, outpath);
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}
