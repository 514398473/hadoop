package com.xz.bigdata.mapreduce.mapjoin;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	private Map<String, String> map = new HashMap<String, String>();
	private Text text = new Text();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		List<String> lines = IOUtils.readLines(new FileInputStream("pdts.txt"));
		if (CollectionUtils.isNotEmpty(lines)) {
			for (String line : lines) {
				String[] fields = line.split(",");
				map.put(fields[0], fields[1]);
			}
		}
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] fields = value.toString().split("\t");
		String pname = map.get(fields[1]);
		text.set(value.toString() + "\t" + pname);
		context.write(text, NullWritable.get());
	}

}
