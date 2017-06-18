package com.xz.bigdata.mapreduce.fans;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FansOne {

	static class FansOneMapper extends Mapper<LongWritable, Text, Text, Text> {

		private Text k = new Text();
		private Text v = new Text();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] personAndFriends = value.toString().split(":");
			String person = personAndFriends[0];
			String[] friends = personAndFriends[1].split(",");
			v.set(person);
			for (String friend : friends) {
				k.set(friend);
				context.write(k, v);
			}
		}

	}

	static class FansOneReducer extends Reducer<Text, Text, Text, Text> {

		private Text v = new Text();

		@Override
		protected void reduce(Text friend, Iterable<Text> persons, Context context)
				throws IOException, InterruptedException {
			StringBuffer stringBuffer = new StringBuffer();
			for (Text person : persons) {
				stringBuffer.append(person.toString()).append(",");
			}
			stringBuffer.deleteCharAt(stringBuffer.length() - 1);
			v.set(stringBuffer.toString());
			context.write(friend, v);
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		FileSystem fileSystem = FileSystem.get(configuration);
		Job job = Job.getInstance(configuration);
		job.setJarByClass(FansOne.class);
		job.setMapperClass(FansOneMapper.class);
		job.setReducerClass(FansOneReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path("E:/input"));
		Path outputPath = new Path("/E:/output");
		if (fileSystem.exists(outputPath)) {
			fileSystem.delete(outputPath, true);
		}
		FileOutputFormat.setOutputPath(job, outputPath);

		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}
