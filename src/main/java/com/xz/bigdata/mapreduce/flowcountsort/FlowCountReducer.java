/**
 * Copyright © 1998-2017, Glodon Inc. All Rights Reserved.
 */
package com.xz.bigdata.mapreduce.flowcountsort;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 此处填写类简介
 * <p>
 * 此处填写类说明
 * </p>
 * 
 * @author xuz-d
 * @since jdk1.6 2017年6月14日
 */

public class FlowCountReducer extends Reducer<Flow, Text, Text, Flow> {

	@Override
	protected void reduce(Flow key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		context.write(values.iterator().next(), key);
	}

}
