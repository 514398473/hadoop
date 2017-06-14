/**
 * Copyright © 1998-2017, Glodon Inc. All Rights Reserved.
 */
package com.xz.bigdata.mapruduce.flowcountsort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * 此处填写类简介
 * <p>
 * 此处填写类说明
 * </p>
 * 
 * @author xuz-d
 * @since jdk1.6 2017年6月14日
 */

public class Flow implements WritableComparable<Flow> {

	private long upflow;
	private long downflow;
	private long sumflow;
	private String phone;

	/**
	 * {@inheritDoc}
	 * 
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upflow);
		out.writeLong(downflow);
		out.writeLong(sumflow);
		out.writeUTF(phone);

	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		this.upflow = in.readLong();
		this.downflow = in.readLong();
		this.sumflow = in.readLong();
		this.phone = in.readUTF();
	}

	public long getUpflow() {
		return upflow;
	}

	public void setUpflow(long upflow) {
		this.upflow = upflow;
	}

	public long getDownflow() {
		return downflow;
	}

	public void setDownflow(long downflow) {
		this.downflow = downflow;
	}

	public long getSumflow() {
		return sumflow;
	}

	public void setSumflow(long sumflow) {
		this.sumflow = sumflow;
	}

	public String getPhone() {
		return phone;
	}

	public void setPhone(String phone) {
		this.phone = phone;
	}

	public Flow() {
	}

	public Flow(long upflow, long downflow) {
		this.upflow = upflow;
		this.downflow = downflow;
		this.sumflow = upflow + downflow;
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return phone + "\t" + upflow + "\t" + downflow + "\t" + sumflow;
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(Flow flow) {
		return this.sumflow>flow.getSumflow()?-1:1;
	}

	/**  
	 * Flow
	 * @param upflow
	 * @param downflow
	 * @param sumflow
	 * @param phone    
	 */
	public Flow(long upflow, long downflow, String phone) {
		this.upflow = upflow;
		this.downflow = downflow;
		this.sumflow = upflow + downflow;
		this.phone = phone;
	}

}
