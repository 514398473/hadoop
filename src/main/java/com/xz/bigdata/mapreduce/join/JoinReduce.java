package com.xz.bigdata.mapreduce.join;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReduce extends Reducer<Text, Join, Join, NullWritable> {

	@Override
	protected void reduce(Text arg0, Iterable<Join> arg1, Context context) throws IOException, InterruptedException {
		Join productJoin = new Join();
		List<Join> list = new ArrayList<Join>();

		for (Join join : arg1) {
			if ("1".equals(join.getFlag())) {
				try {
					BeanUtils.copyProperties(productJoin, join);
				} catch (IllegalAccessException | InvocationTargetException e) {
					e.printStackTrace();
				}
			} else {
				Join orderJoin = new Join();
				try {
					BeanUtils.copyProperties(orderJoin, join);
					list.add(orderJoin);
				} catch (IllegalAccessException | InvocationTargetException e) {
					e.printStackTrace();
				}
			}
		}

		for (Join join : list) {
			join.setPname(productJoin.getPname());
			join.setCategoryId(productJoin.getCategoryId());
			join.setPrice(productJoin.getPrice());
			context.write(join, NullWritable.get());
		}
	}

}
