/**
 * Copyright © 1998-2017, Glodon Inc. All Rights Reserved.
 */
package com.xz.bigdata.hbase.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hbase测试类
 * <p>
 * Hbase测试类
 * </p>
 * 
 * @author xuz-d
 * @since jdk1.6 2017年4月27日
 */
public class HbaseTest {

	private static final Logger LOGGER = LoggerFactory.getLogger(HbaseTest.class);
	/**
	 * 配置
	 */
	private static Configuration configuration;

	private static HBaseAdmin hBaseAdmin;

	static {
		configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.quorum", "node1,node2,node3");
		try {
			hBaseAdmin = new HBaseAdmin(configuration);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		// createTable("table1",new String[]{"family1"});
		// Map<String, String> hashMap = new HashMap<String, String>();
		// hashMap.put("23432", "电饭锅");
		// hashMap.put("斯蒂芬", "斯蒂芬");
		// hashMap.put("规范化", "你把");
		// hashMap.put("CVBS", "23");
		// hashMap.put("东方红", "vcb");
		// hashMap.put("电饭锅", "撒地方");
		// insertData("table1", "family3", "149234234237357", hashMap);
		// addColumnFamily("table1", new String[]{"family6"});
		// dropTable("xuzheng");
		// deleteRowByRowKey("table1", "1493283767357");
		// List<HbaseResult> allData = getAllData("table1");
		// for (HbaseResult hbaseResult : allData) {
		// System.out.println(hbaseResult.toString());
		// }
		// List<HbaseResult> dataByRowKey = getDataByRowKey("table1",
		// "149234234237357");
		// for (HbaseResult hbaseResult : dataByRowKey) {
		// System.out.println(hbaseResult.toString());
		// }
		List<HbaseResult> dateByColumn = getDateByColumn("table1", "family3", "CVBS");
		for (HbaseResult hbaseResult : dateByColumn) {
			System.out.println(hbaseResult.toString());
		}
	}

	/**
	 * 新建表
	 * 
	 * @param tableName
	 * @param familyNames
	 */
	public static void createTable(String tableName, String[] familyNames) {
		try {
			if (StringUtils.isBlank(tableName) || null == familyNames || familyNames.length == 0) {
				LOGGER.error("参数为空或无值！新建失败！");
				return;
			}
			if (hBaseAdmin.tableExists(tableName)) {
				hBaseAdmin.disableTable(tableName);
				hBaseAdmin.deleteTable(tableName);
				LOGGER.warn(tableName + "表已经存在！已经被删除");
			}
			HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
			for (String familyName : familyNames) {
				desc.addFamily(new HColumnDescriptor(familyName));
			}
			hBaseAdmin.createTable(desc);
			LOGGER.info(tableName + "创建成功！");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 添加数据
	 * 
	 * @param tableName
	 * @param familyName
	 * @param rowKey
	 * @param keyValue
	 */
	@SuppressWarnings("resource")
	public static void insertData(String tableName, String familyName, String rowKey, Map<String, String> keyValue) {
		if (StringUtils.isBlank(tableName) || StringUtils.isBlank(familyName) || StringUtils.isBlank(rowKey) || null == keyValue || keyValue.size() == 0) {
			LOGGER.error("参数为空或无值！插入失败！");
			return;
		}
		try {
			HTable hTable = new HTable(configuration, tableName);
			List<Put> puts = new ArrayList<Put>();
			for (Entry<String, String> entry : keyValue.entrySet()) {
				Put put = new Put(rowKey.getBytes());
				put.add(familyName.getBytes(), entry.getKey().getBytes(), entry.getValue().getBytes());
				puts.add(put);
			}
			hTable.put(puts);
			LOGGER.info("添加数据成功！");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 创建好表后，添加列族
	 * 
	 * @param tableName
	 * @param familyName
	 */
	public static void addColumnFamily(String tableName, String[] familyNames) {
		if (StringUtils.isBlank(tableName) || null == familyNames || familyNames.length == 0) {
			LOGGER.error("参数为空或无值！添加失败！");
			return;
		}
		try {
			@SuppressWarnings("resource")
			HTableDescriptor hTableDescriptor = new HTableDescriptor(new HTable(configuration, tableName).getTableDescriptor());
			for (String familyName : familyNames) {
				HColumnDescriptor family = new HColumnDescriptor(familyName);
				hTableDescriptor.addFamily(family);
			}
			hBaseAdmin.disableTable(tableName);
			hBaseAdmin.modifyTable(tableName, hTableDescriptor);
			hBaseAdmin.enableTable(tableName);
			LOGGER.info("添加列族成功！");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 删除表
	 * 
	 * @param tableName
	 */
	public static void dropTable(String tableName) {
		if (StringUtils.isBlank(tableName)) {
			LOGGER.error("参数为空或无值！删除失败！");
			return;
		}
		try {
			hBaseAdmin.disableTable(tableName);
			hBaseAdmin.deleteTable(tableName);
			LOGGER.info(tableName + "删除成功！");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 根据rowkey删除数据
	 * 
	 * @param tableName
	 * @param rowkey
	 */
	public static void deleteRowByRowKey(String tableName, String rowKey) {
		if (StringUtils.isBlank(tableName) || StringUtils.isBlank(rowKey)) {
			LOGGER.error("参数为空或无值！删除失败！");
			return;
		}
		try {
			@SuppressWarnings("resource")
			HTable hTable = new HTable(configuration, tableName);
			hTable.delete(new Delete(rowKey.getBytes()));
			LOGGER.info("删除成功！");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 获取表中的所有数据
	 * 
	 * @param tableName
	 */
	public static List<HbaseResult> getAllData(String tableName) {
		if (StringUtils.isBlank(tableName)) {
			LOGGER.error("参数为null或无值，不能查询！");
			return null;
		}
		ResultScanner scanner = null;
		try {
			List<HbaseResult> hbaseResults = new ArrayList<HbaseResult>();
			@SuppressWarnings("resource")
			HTable hTable = new HTable(configuration, tableName);
			scanner = hTable.getScanner(new Scan());
			for (Result result : scanner) {
				String rowkey = new String(result.getRow());
				Cell[] rawCells = result.rawCells();
				for (Cell cell : rawCells) {
					HbaseResult hbaseResult = new HbaseResult();
					hbaseResult.setRowKey(rowkey);
					hbaseResult.setFamily(new String(CellUtil.cloneFamily(cell)));
					hbaseResult.setQualifier(new String(CellUtil.cloneQualifier(cell)));
					hbaseResult.setValue(new String(CellUtil.cloneValue(cell)));
					hbaseResults.add(hbaseResult);
				}
			}
			LOGGER.info("查询成功！");
			return hbaseResults;
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (null != scanner) {
				scanner.close();
			}
		}
		return null;
	}

	/**
	 * 根据tableName 和 rowKey的值获取该行的数据
	 * 
	 * @param tableName
	 * @param rowKey
	 * @return
	 */
	public static List<HbaseResult> getDataByRowKey(String tableName, String rowKey) {
		if (StringUtils.isBlank(tableName) || StringUtils.isBlank(rowKey)) {
			LOGGER.error("参数为null或无值，不能查询！");
			return null;
		}
		try {
			List<HbaseResult> hbaseResults = new ArrayList<HbaseResult>();
			@SuppressWarnings("resource")
			HTable hTable = new HTable(configuration, tableName);
			Get get = new Get(rowKey.getBytes());
			Result result = hTable.get(get);
			Cell[] rawCells = result.rawCells();
			for (Cell cell : rawCells) {
				HbaseResult hbaseResult = new HbaseResult();
				hbaseResult.setRowKey(rowKey);
				hbaseResult.setFamily(new String(CellUtil.cloneFamily(cell)));
				hbaseResult.setQualifier(new String(CellUtil.cloneQualifier(cell)));
				hbaseResult.setValue(new String(CellUtil.cloneValue(cell)));
				hbaseResults.add(hbaseResult);
			}
			LOGGER.info("查询成功！");
			return hbaseResults;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 根据tableName和指定columnFamily:qualifier 来获取查询的数据
	 * 
	 * @param tableName
	 * @param columnFamily
	 * @param qualifier
	 * @return
	 */
	public static List<HbaseResult> getDateByColumn(String tableName, String columnFamily, String qualifier) {
		if (StringUtils.isBlank(tableName) || StringUtils.isBlank(columnFamily) || StringUtils.isBlank(qualifier)) {
			LOGGER.error("参数为null或无值，不能查询！");
			return null;
		}
		ResultScanner scanner = null;
		try {
			List<HbaseResult> hbaseResults = new ArrayList<HbaseResult>();
			@SuppressWarnings("resource")
			HTable hTable = new HTable(configuration, tableName);
			Scan scan = new Scan();
			scan.addColumn(columnFamily.getBytes(), qualifier.getBytes());
			scanner = hTable.getScanner(scan);
			for (Result result : scanner) {
				String rowkey = new String(result.getRow());
				Cell[] rawCells = result.rawCells();
				for (Cell cell : rawCells) {
					HbaseResult hbaseResult = new HbaseResult();
					hbaseResult.setRowKey(rowkey);
					hbaseResult.setFamily(new String(CellUtil.cloneFamily(cell)));
					hbaseResult.setQualifier(new String(CellUtil.cloneQualifier(cell)));
					hbaseResult.setValue(new String(CellUtil.cloneValue(cell)));
					hbaseResults.add(hbaseResult);
				}
			}
			LOGGER.info("查询成功！");
			return hbaseResults;
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (null != scanner) {
				scanner.close();
			}
		}
		return null;
	}
}
