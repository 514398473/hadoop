package com.xz.bigdata.mapreduce.fans;

import java.io.IOException;
import java.util.Arrays;

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

public class FansTwo {

	static class FansTwoMapper extends Mapper<LongWritable, Text, Text, Text> {

		private Text k = new Text();
		private Text v = new Text();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] friendAndPersons = value.toString().split("\t");
			String friend = friendAndPersons[0];
			String[] persons = friendAndPersons[1].split(",");
			Arrays.sort(persons);
			for (int i = 0; i < persons.length - 1; i++) {
				for (int j = i + 1; j < persons.length; j++) {
					k.set(persons[i] + "-" + persons[j]);
					v.set(friend);
					context.write(k, v);
				}
			}
		}

	}

	static class FansTwoReducer extends Reducer<Text, Text, Text, Text> {

		private Text v = new Text();

		@Override
		protected void reduce(Text key, Iterable<Text> friends, Context context)
				throws IOException, InterruptedException {
			StringBuffer stringBuffer = new StringBuffer();
			for (Text friend : friends) {
				stringBuffer.append(friend.toString()).append(",");
			}
			stringBuffer.deleteCharAt(stringBuffer.length() - 1);
			v.set(stringBuffer.toString());
			context.write(key, v);
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		FileSystem fileSystem = FileSystem.get(configuration);
		Job job = Job.getInstance(configuration);
		job.setJarByClass(FansTwo.class);
		job.setMapperClass(FansTwoMapper.class);
		job.setReducerClass(FansTwoReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path("E:/output"));
		Path outputPath = new Path("/E:/output1");
		if (fileSystem.exists(outputPath)) {
			fileSystem.delete(outputPath, true);
		}
		FileOutputFormat.setOutputPath(job, outputPath);

		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}

}
