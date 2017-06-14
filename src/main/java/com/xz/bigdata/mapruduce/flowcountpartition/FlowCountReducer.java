/**
 * Copyright © 1998-2017, Glodon Inc. All Rights Reserved.
 */
package com.xz.bigdata.mapruduce.flowcountpartition;

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

public class FlowCountReducer extends Reducer<Text, Flow, Text, Flow> {

	/**
	 * {@inheritDoc}
	 * 
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object,
	 *      java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void reduce(Text key, Iterable<Flow> values, Context context) throws IOException, InterruptedException {
		long sumUpFlow = 0;
		long sumDownFlow = 0;
		for (Flow flow : values) {
			sumUpFlow += flow.getUpflow();
			sumDownFlow += flow.getDownflow();
		}
		context.write(key, new Flow(sumUpFlow, sumDownFlow));
	}

}
