package com.bfd.kafka.test;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hbase.generated.regionserver.regionserver_jsp;
import org.junit.experimental.theories.Theories;

import com.bfd.kafka2file.ConsumerJob;

import tech.spiro.addrparser.common.RegionInfo;
import tech.spiro.addrparser.io.RegionDataInput;
import tech.spiro.addrparser.io.file.JSONFileRegionDataInput;
import tech.spiro.addrparser.parser.Location;
import tech.spiro.addrparser.parser.LocationParserEngine;
import tech.spiro.addrparser.parser.ParserEngineException;

public class TestDemo {
	public static void main(String[] args) throws IOException {
		/*Date date = new Date();
		SimpleDateFormat sdFormat = new SimpleDateFormat("yyyyMMddHH");
		String dateString = sdFormat.format(date);
		System.out.println(dateString);
		StringBuffer stringBuffer = new StringBuffer();
		String path = "d:\\test\\10.txt";
    	File outFile = new File(path);
    	if(!outFile.exists()){
    		outFile.createNewFile();
    	}
    	BufferedWriter writer = new BufferedWriter(new FileWriter(path,true));
    	if(outFile.length()==0){
    	writer.write("test01");
        writer.close();
    	}else{
    		writer.write("\r\ntest03");
    		writer.close();
    	}*/
		// china-region.json文件作为基础数据
		/*InputStream inputStream = TestDemo.class.getClassLoader().getResourceAsStream("china-region.json");
		//System.out.println(fileName);
		String fileName = IOU*/
		String path = ConsumerJob.class.getResource("/china-region.json").getPath();
		RegionDataInput regionDataInput = new JSONFileRegionDataInput(path);

		// 创建并初始化位置解析引擎，一般配置为全局单例
		LocationParserEngine engine = new LocationParserEngine(regionDataInput);
		// 初始化，加载数据，比较耗时
		try {
			long begin = System.currentTimeMillis();
			engine.init();
			long end = System.currentTimeMillis();
			System.out.println("加载地图耗时"+(end-begin)/1000+"s");
		} catch (ParserEngineException e) {
			// TODO Auto-generated catch block
			System.out.println("地图初始化异常...");
			e.printStackTrace();
		}

		// 执行解析操作
		Location location = engine.parse(85.72034,41.84937);

		// 获取省市区信息
		String provInfo = location.getProv().getName();
		String cityInfo = location.getCity().getName();
		String districtInfo = location.getDistrict().getName();
		
		System.out.println(provInfo+":"+cityInfo+":"+districtInfo);
		
	}
}
