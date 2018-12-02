package com.bfd.kafka.pojo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class CarInfoWritable implements Writable,Comparable<CarInfoWritable> {
	private String deviceid;//车辆id
	private String region;//区域
	private Long date;//时间戳
	
	

	public String getDeviceid() {
		return deviceid;
	}

	public void setDeviceid(String deviceid) {
		this.deviceid = deviceid;
	}

	public String getRegion() {
		return region;
	}

	public void setRegion(String region) {
		this.region = region;
	}

	public Long getDate() {
		return date;
	}

	public void setDate(Long date) {
		this.date = date;
	}

	@Override
	public String toString() {
		return "CarInfoWritable [deviceid=" + deviceid + ", region=" + region + ", date=" + date + "]";
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(deviceid);
		out.writeUTF(region);
		out.writeLong(date);

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.deviceid = in.readUTF();
		this.region = in.readUTF();
		this.date = in.readLong();
		
	}

	@Override
	public int compareTo(CarInfoWritable carInfo) {
		int returnData = this.date >carInfo.getDate()?1:-1;
		return returnData;
	}

}
