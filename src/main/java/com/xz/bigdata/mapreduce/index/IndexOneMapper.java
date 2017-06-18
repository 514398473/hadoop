package com.xz.bigdata.mapreduce.index;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class IndexOneMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private Text text = new Text();
	private IntWritable count = new IntWritable(1);

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] values = value.toString().split(" ");
		FileSplit fileSplit = (FileSplit) context.getInputSplit();
		String fileName = fileSplit.getPath().getName();
		for (String word : values) {
			text.set(word + "-" + fileName);
			context.write(text, count);
		}
	}

}
