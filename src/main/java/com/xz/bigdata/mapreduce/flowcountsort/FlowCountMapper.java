/**
* Copyright © 1998-2017, Glodon Inc. All Rights Reserved.
*/
package com.xz.bigdata.mapreduce.flowcountsort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * 此处填写类简介
 * <p>
 * 此处填写类说明
 * </p>
 * 
 * @author xuz-d
 * @since jdk1.6 2017年6月14日
 * 
 */

public class FlowCountMapper extends Mapper<LongWritable, Text, Flow, Text> {

	private Flow flow = new Flow();
	private Text text = new Text();

	/**
	 * {@inheritDoc}
	 * 
	 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object,
	 *      java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] values = line.split("\t");
		flow.set(Long.valueOf(values[1]), Long.valueOf(values[2]));
		text.set(values[0]);
		context.write(flow, text);
	}

}
