package com.xz.bigdata.mapruduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int count = 0;
		for (IntWritable intWritable : values) {
			count += intWritable.get();
		}
		context.write(new Text(key), new IntWritable(count));
	}

}
