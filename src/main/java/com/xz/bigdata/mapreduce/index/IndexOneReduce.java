package com.xz.bigdata.mapreduce.index;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IndexOneReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

	private IntWritable sum = new IntWritable();

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int count = 0;
		for (IntWritable intWritable : values) {
			count += intWritable.get();
		}
		sum.set(count);
		context.write(key, sum);
	}

}
