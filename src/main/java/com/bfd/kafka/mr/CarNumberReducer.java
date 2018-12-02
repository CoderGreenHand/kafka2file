package com.bfd.kafka.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.bfd.kafka.pojo.CarInfoWritable;

/**
 * 
 * @author jiaze.yuan
 * @see   将map同一个deviceid的信息进行合并，
 *  然后按照timestamp进行排序
 * 	同一辆车的上个时间点和下个时间点进行比较
 * 	如何两次的区域相同则跳过，如果不同，则将两次的区域对应的map<k,v>
 *	的value进行+1操作
 */

public class CarNumberReducer extends Reducer<Text, CarInfoWritable, Text, IntWritable> {
	private Map<String,Integer> carCountMap = new HashMap<String,Integer>();
	
	@Override
	protected void reduce(Text key, Iterable<CarInfoWritable> values, Context context)
			throws IOException, InterruptedException {
		String region ="";//存放临时区域
		Iterator<CarInfoWritable> iterator = values.iterator();
		List<CarInfoWritable> list = new ArrayList<CarInfoWritable>();
		while (iterator.hasNext()) {
		CarInfoWritable item = iterator.next();
		list.add(item);
		}
		Collections.sort(list);//对同一辆车的value按时间进行排序
		//遍历集合中的元素，判断两个连续时间点的车是否在同一区域
		for (CarInfoWritable carInfo : list) {
			if (carInfo.getRegion()!=null && !carInfo.getRegion().equals("")) {
				if ("".equals(region)) {
					region = carInfo.getRegion();
				}else if (region.equals(carInfo.getRegion())) { 
					continue; //两次相邻时间所对应的区域相等的时候，跳过本次循环
				}else {
					//两次相邻时间所对应的区域不相等的时候，将region更新为本次
					//循环对应的区域，然后两个区域对应的次数+1
					//判断map中是否已经存在这两个region对应的值，如果存在则对
					//这两个key的value进行分别+1操作，并对region进行重新赋值
					if (carCountMap.containsKey(region)) {
						carCountMap.put(region,carCountMap.get(region)+1);
						
					}else{
						carCountMap.put(region, 1);
					}
					if(carCountMap.containsKey(carInfo.getRegion())){
						//判断下一个时间点的region是否在map<k,v>中，如果在，同样+1，
						//否则添加
						carCountMap.put(carInfo.getRegion(), carCountMap.get(carInfo.getRegion())+1);
					}else{
						carCountMap.put(carInfo.getRegion(), 1);
					}
					region = carInfo.getRegion();//更新region	
				}
				
			}
		}
		
	}
	
	@Override
	protected void cleanup(Reducer<Text, CarInfoWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		//一次将所有map的统计结果输出，21w辆车
		for (Map.Entry<String, Integer> entry: carCountMap.entrySet()) {
			context.write(new Text(entry.getKey()),new IntWritable(entry.getValue()));
		}
		super.cleanup(context);
	}

}
