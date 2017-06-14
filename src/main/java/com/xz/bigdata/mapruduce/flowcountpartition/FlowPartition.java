/**
 * Copyright © 1998-2017, Glodon Inc. All Rights Reserved.
 */
package com.xz.bigdata.mapruduce.flowcountpartition;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 此处填写类简介
 * <p>
 * 此处填写类说明
 * </p>
 * 
 * @author xuz-d
 * @since jdk1.6 2017年6月14日
 */

public class FlowPartition extends Partitioner<Text, Flow> {

	private static Map<String, Integer> locationMap = new HashMap<String, Integer>();

	static {
		locationMap.put("136", 0);
		locationMap.put("137", 1);
		locationMap.put("138", 2);
		locationMap.put("139", 3);
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see org.apache.hadoop.mapreduce.Partitioner#getPartition(java.lang.Object,
	 *      java.lang.Object, int)
	 */
	@Override
	public int getPartition(Text key, Flow value, int numPartitions) {
		String prefix = key.toString().substring(0, 3);
		Integer location = locationMap.get(prefix);
		return location == null ? 4 : location;
	}

}
