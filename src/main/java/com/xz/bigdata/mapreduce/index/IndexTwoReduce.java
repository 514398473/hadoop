package com.xz.bigdata.mapreduce.index;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IndexTwoReduce extends Reducer<Text, Text, Text, Text> {

	private Text v = new Text();

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		StringBuilder stringBuilder = new StringBuilder();
		for (Text value : values) {
			stringBuilder.append(value.toString()).append("\t");
		}
		v.set(stringBuilder.toString());
		context.write(key, v);
	}

}
