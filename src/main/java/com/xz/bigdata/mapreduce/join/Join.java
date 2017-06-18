package com.xz.bigdata.mapreduce.join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Join implements Writable {

	private int id;
	private String date;
	private String pid;
	private int amount;
	private String pname;
	private String categoryId;
	private float price;
	private String flag;

	public void set(int id, String date, String pid, int amount, String pname, String categoryId, float price,
			String flag) {
		this.id = id;
		this.date = date;
		this.pid = pid;
		this.amount = amount;
		this.pname = pname;
		this.categoryId = categoryId;
		this.price = price;
		this.flag = flag;
	}

	public String getFlag() {
		return flag;
	}

	public void setFlag(String flag) {
		this.flag = flag;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getPid() {
		return pid;
	}

	public void setPid(String pid) {
		this.pid = pid;
	}

	public int getAmount() {
		return amount;
	}

	public void setAmount(int amount) {
		this.amount = amount;
	}

	public String getPname() {
		return pname;
	}

	public void setPname(String pname) {
		this.pname = pname;
	}

	public String getCategoryId() {
		return categoryId;
	}

	public void setCategoryId(String categoryId) {
		this.categoryId = categoryId;
	}

	public float getPrice() {
		return price;
	}

	public void setPrice(float price) {
		this.price = price;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(id);
		out.writeUTF(date);
		out.writeUTF(pid);
		out.writeInt(amount);
		out.writeUTF(pname);
		out.writeUTF(categoryId);
		out.writeFloat(price);
		out.writeUTF(flag);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.id = in.readInt();
		this.date = in.readUTF();
		this.pid = in.readUTF();
		this.amount = in.readInt();
		this.pname = in.readUTF();
		this.categoryId = in.readUTF();
		this.price = in.readFloat();
		this.flag = in.readUTF();
	}

	@Override
	public String toString() {
		return "id=" + id + ", date=" + date + ", pid=" + pid + ", amount=" + amount + ", pname=" + pname
				+ ", categoryId=" + categoryId + ", price=" + price;
	}

}
