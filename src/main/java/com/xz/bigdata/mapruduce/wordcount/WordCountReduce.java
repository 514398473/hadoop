package com.xz.bigdata.mapruduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	protected void reduce(Text word, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int count = 0;
		for (IntWritable intWritable : values) {
			count += intWritable.get();
		}
		context.write(new Text(word), new IntWritable(count));
	}

}
