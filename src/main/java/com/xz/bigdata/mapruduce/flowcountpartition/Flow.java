/**
 * Copyright © 1998-2017, Glodon Inc. All Rights Reserved.
 */
package com.xz.bigdata.mapruduce.flowcountpartition;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * 此处填写类简介
 * <p>
 * 此处填写类说明
 * </p>
 * 
 * @author xuz-d
 * @since jdk1.6 2017年6月14日
 */

public class Flow implements Writable {

	private long upflow;
	private long downflow;
	private long sumflow;

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
		return upflow + "\t" + downflow + "\t" + sumflow;
	}

}
