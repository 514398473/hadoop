package com.xz.bigdata.hbase.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestHbase {

	private Configuration configuration;
	private Connection connection;
	private Table table;

	@Before
	public void init() throws Exception {
		configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.quorum", "study1,study2,study3");// zookeeper地址
		configuration.set("hbase.zookeeper.property.clientPort", "2181");// zookeeper端口
		connection = ConnectionFactory.createConnection(configuration);
		table = connection.getTable(TableName.valueOf("niubi"));
	}

	/**
	 * 创建表
	 * 
	 * @throws Exception
	 */
	@Test
	public void createTable() throws Exception {
		TableName tableName = TableName.valueOf("niubi");
		Admin admin = connection.getAdmin();
		if (admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}
		HTableDescriptor desc = new HTableDescriptor(tableName);
		HColumnDescriptor family = new HColumnDescriptor("info1");
		desc.addFamily(family);
		admin.createTable(desc);
	}

	/**
	 * 删除表
	 * 
	 * @throws Exception
	 */
	@Test
	public void deleteTable() throws Exception {
		TableName tableName = TableName.valueOf("niubi");
		Admin admin = connection.getAdmin();
		if (admin.tableExists(tableName)) {
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}
	}

	/**
	 * 添加数据
	 * 
	 * @throws Exception
	 */
	@Test
	public void insertData() throws Exception {
		table.setAutoFlushTo(false);
		table.setWriteBufferSize(536870912);
		List<Put> puts = new ArrayList<Put>();
		for (int i = 0; i < 50; i++) {
			Put put = new Put(Bytes.toBytes("rowkey" + i));
			put.add(Bytes.toBytes("info1"), Bytes.toBytes("name"), Bytes.toBytes("x" + i));
			put.add(Bytes.toBytes("info1"), Bytes.toBytes("password"), Bytes.toBytes("x" + i));
			puts.add(put);
		}
		table.put(puts);
		table.flushCommits();
	}

	/**
	 * 修改数据
	 * 
	 * @throws Exception
	 */
	@Test
	public void updateData() throws Exception {
		Get get = new Get(Bytes.toBytes("rowkey1"));
		if (table.exists(get)) {
			Put put = new Put(Bytes.toBytes("rowkey1"));
			put.add(Bytes.toBytes("info1"), Bytes.toBytes("name"), Bytes.toBytes("刘能"));
			table.put(put);
		}
	}

	/**
	 * 删除数据
	 * 
	 * @throws Exception
	 */
	@Test
	public void deleteData() throws Exception {
		Delete delete = new Delete(Bytes.toBytes("rowkey0"));
		table.delete(delete);
	}

	/**
	 * 查询单挑数据
	 * 
	 * @throws Exception
	 */
	@Test
	public void querySingleData() throws Exception {
		Get get = new Get(Bytes.toBytes("rowkey5"));
		Result result = table.get(get);
		byte[] value = result.getValue(Bytes.toBytes("info1"), Bytes.toBytes("name"));
		System.out.println(Bytes.toString(value));
	}

	/**
	 * 全表扫描
	 * 
	 * @throws Exception
	 */
	@Test
	public void scanData() throws Exception {
		Scan scan = new Scan();
		// scan.addFamily(Bytes.toBytes("info1"));
		// scan.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("password"));
		// scan.setTimeStamp(1498737229105L);
		// scan.setStartRow(Bytes.toBytes("rowkey3"));
		// scan.setStopRow(Bytes.toBytes("rowkey5"));
		ResultScanner resultScanner = table.getScanner(scan);
		for (Result result : resultScanner) {
			byte[] value = result.getValue(Bytes.toBytes("info1"), Bytes.toBytes("password"));
			System.out.println(Bytes.toString(value));
		}
	}

	/**
	 * 列值过滤器
	 * 
	 * @throws Exception
	 */
	@Test
	public void SingleColumnValueFilter() throws Exception {
		Scan scan = new Scan();
		Filter filter = new SingleColumnValueFilter(Bytes.toBytes("info1"), Bytes.toBytes("password"), CompareOp.EQUAL,
				Bytes.toBytes("x6"));
		scan.setFilter(filter);
		ResultScanner resultScanner = table.getScanner(scan);
		for (Result result : resultScanner) {
			byte[] value = result.getValue(Bytes.toBytes("info1"), Bytes.toBytes("password"));
			System.out.println(Bytes.toString(value));
		}
	}

	/**
	 * rowkey过滤器
	 * 
	 * @throws Exception
	 */
	@Test
	public void rowFilter() throws Exception {
		Scan scan = new Scan();
		Filter filter = new RowFilter(CompareOp.EQUAL, new RegexStringComparator("rowkey4"));
		scan.setFilter(filter);
		ResultScanner resultScanner = table.getScanner(scan);
		for (Result result : resultScanner) {
			byte[] value = result.getValue(Bytes.toBytes("info1"), Bytes.toBytes("password"));
			System.out.println(Bytes.toString(value));
		}
	}

	/**
	 * 匹配列名前缀过滤器
	 * 
	 * @throws Exception
	 */
	@Test
	public void prefixFilter() throws Exception {
		Scan scan = new Scan();
		Filter filter = new ColumnPrefixFilter(Bytes.toBytes("nam"));
		scan.setFilter(filter);
		ResultScanner resultScanner = table.getScanner(scan);
		for (Result result : resultScanner) {
			byte[] value = result.getValue(Bytes.toBytes("info1"), Bytes.toBytes("name"));
			System.out.println(Bytes.toString(value));
		}
	}

	/**
	 * 过滤器集合
	 * 
	 * @throws Exception
	 */
	@Test
	public void filters() throws Exception {
		Scan scan = new Scan();
		FilterList filterList = new FilterList(Operator.MUST_PASS_ALL);
		Filter filter1 = new RowFilter(CompareOp.EQUAL, new RegexStringComparator("rowkey2"));
		Filter filter2 = new ColumnPrefixFilter(Bytes.toBytes("nam"));
		filterList.addFilter(filter1);
		filterList.addFilter(filter2);
		scan.setFilter(filterList);
		ResultScanner resultScanner = table.getScanner(scan);
		for (Result result : resultScanner) {
			byte[] value = result.getValue(Bytes.toBytes("info1"), Bytes.toBytes("name"));
			System.out.println(Bytes.toString(value));
		}
	}

	@After
	public void after() throws Exception {
		table.close();
		connection.close();
	}

}