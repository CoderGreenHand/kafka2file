package com.bfd.kafka.mr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.bfd.kafka.pojo.CarInfoWritable;

/**
 * 
 * @author jiaze.yuan
 * 读取hive中从kafka接入的原始数据
 * 将deviceid，timestamp，region读取出来
 * 以deviceid作为map的key
 * reduce中按照同一辆车的timestamp进行排序
 *
 */

public class CarNumberMapper extends Mapper<LongWritable, Text, Text, CarInfoWritable>{
	protected void map(LongWritable key, Text value, Context context) throws IOException ,InterruptedException {
		
		CarInfoWritable carInfo = new CarInfoWritable();
		String[] line = value.toString().split("\t");
		if (line.length>=1) {
			//数组的下标可能需要改变
			carInfo.setDeviceid(line[0]);
			carInfo.setDate(Long.valueOf(line[1]));
			carInfo.setRegion(line[8]);
		}
		Text outKey = new Text(carInfo.getDeviceid());
		context.write(outKey, carInfo);
		
	}
}
