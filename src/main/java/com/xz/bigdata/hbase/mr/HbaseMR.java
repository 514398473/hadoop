package com.xz.bigdata.hbase.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

public class HbaseMR {

	static class HbaseMapper extends TableMapper<Text, IntWritable> {

		private Text text = new Text();
		private IntWritable intWritable = new IntWritable(1);

		@Override
		protected void map(ImmutableBytesWritable key, Result value, Context context)
				throws IOException, InterruptedException {
			String line = Bytes.toString(value.getValue(Bytes.toBytes("info1"), Bytes.toBytes("name")));
			String[] words = line.split(" ");
			for (String word : words) {
				text.set(word);
				context.write(text, intWritable);
			}
		}
	}

	static class HbaseReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable intWritable : values) {
				sum += intWritable.get();
			}
			Put put = new Put(Bytes.toBytes(key.toString()));
			put.add(Bytes.toBytes("info1"), Bytes.toBytes("name"), Bytes.toBytes(String.valueOf(sum)));
			context.write(new ImmutableBytesWritable(Bytes.toBytes(key.toString())), put);
		}

	}

	public static void main(String[] args) throws Exception {
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration, "hbasewordcount");
		job.setJar("C:\\Users\\Administrator\\Desktop\\wc.jar");
		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("name"));
		TableMapReduceUtil.initTableMapperJob(TableName.valueOf("niubi"), scan, HbaseMapper.class, Text.class,
				IntWritable.class, job);
		TableMapReduceUtil.initTableReducerJob("out", HbaseReducer.class, job);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
