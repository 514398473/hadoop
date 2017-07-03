/**
 * Copyright © 1998-2017, Glodon Inc. All Rights Reserved.
 */
package com.xz.bigdata.hbase.test;

/**
 * Hbase返回结果类
 * <p>
 * Hbase返回结果类
 * </p>
 * 
 * @author xuz-d
 * @since jdk1.6 2017年4月28日
 */

public class HbaseResult {

	private String rowKey;

	private String family;

	private String qualifier;

	private String value;

	public String getRowKey() {
		return rowKey;
	}

	public void setRowKey(String rowKey) {
		this.rowKey = rowKey;
	}

	public String getFamily() {
		return family;
	}

	public void setFamily(String family) {
		this.family = family;
	}

	public String getQualifier() {
		return qualifier;
	}

	public void setQualifier(String qualifier) {
		this.qualifier = qualifier;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return "HbaseResult [rowKey=" + rowKey + ", family=" + family + ", qualifier=" + qualifier + ", value=" + value + "]";
	}

}
