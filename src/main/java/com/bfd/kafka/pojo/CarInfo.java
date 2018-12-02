package com.bfd.kafka.pojo;

public class CarInfo {
	private long deviceId;
	private long dateTime;
	private double longi;//经度
	private double lati;//纬度
	private int speed;//速度
	private int status;//状态位
	private String province;//省
	private String city;//市
	//private String region;//区域
	private String regionCode;//区域对应的china-region编码
	public String getRegionCode() {
		return regionCode;
	}
	public void setRegionCode(String regionCode) {
		this.regionCode = regionCode;
	}
	public String getProvince() {
		return province;
	}
	public void setProvince(String province) {
		this.province = province;
	}
	public String getCity() {
		return city;
	}
	public void setCity(String city) {
		this.city = city;
	}

	public CarInfo() {
		// TODO Auto-generated constructor stub
	}
	public CarInfo(String deviceid,String datetime,double longi,double lati,int status,int vin){
		
	}
	public long getDeviceId() {
		return deviceId;
	}
	public void setDeviceId(long deviceId) {
		this.deviceId = deviceId;
	}

	public long getDateTime() {
		return dateTime;
	}
	public void setDateTime(long dateTime) {
		this.dateTime = dateTime;
	}
	public double getLongi() {
		return longi;
	}
	public void setLongi(double longi) {
		this.longi = longi;
	}
	public double getLati() {
		return lati;
	}
	public void setLati(double lati) {
		this.lati = lati;
	}
	public int getStatus() {
		return status;
	}
	public void setStatus(int status) {
		this.status = status;
	}
	public int getSpeed() {
		return speed;
	}
	public void setSpeed(int speed) {
		this.speed = speed;
	}
	@Override
	public String toString() {
		return deviceId + "\t" + dateTime + "\t" + longi + "\t" + lati
				+ "\t" + speed + "\t" + status+ "\t" + province + "\t"
				+ city + "\t" +regionCode ;
	}

}
