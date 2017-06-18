package com.xz.bigdata.mapreduce.join;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class JoinMapper extends Mapper<LongWritable, Text, Text, Join> {

	private Join join = new Join();
	private Text text = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		FileSplit fileSplit = (FileSplit) context.getInputSplit();
		String fileName = fileSplit.getPath().getName();
		String pid = "";
		if (fileName.startsWith("order")) {
			String[] fields = value.toString().split("\t");
			pid = fields[2];
			join.set(Integer.parseInt(fields[0]), fields[1], pid, Integer.parseInt(fields[3]), "", "", 0, "0");
		} else {
			String[] fields = value.toString().split("\t");
			pid = fields[0];
			join.set(0, "", pid, 0, fields[1], fields[2], Float.parseFloat(fields[3]), "1");

		}
		text.set(pid);
		context.write(text, join);
	}

}
