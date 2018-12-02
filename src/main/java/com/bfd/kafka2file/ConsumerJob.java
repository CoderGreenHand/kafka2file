package com.bfd.kafka2file;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.bfd.kafka.pojo.CarInfo;
import com.bfd.kafka.protobuf.JTT808PositionInfoReport.PositionInfoReport;
import com.bfd.kafka.protobuf.JTT808PositionInfoReport.PositionInfoReport.PositionBasicInfo;
import com.bfd.kafka.protobuf.QMDPPackageHead.PackageHead;
import com.bfd.kafka.util.FileUtil;
import com.google.protobuf.ByteString;

import tech.spiro.addrparser.io.RegionDataInput;
import tech.spiro.addrparser.io.file.JSONFileRegionDataInput;
import tech.spiro.addrparser.parser.Location;
import tech.spiro.addrparser.parser.LocationParserEngine;
import tech.spiro.addrparser.parser.ParserEngineException;

public class ConsumerJob {
	private static LocationParserEngine engine = null;
	private static Properties props;
	private static int loscount = 0;
	private final static String T_0000_POSITION = "T_0000_POSITION"; // 808标准 位置信息上报
    
    
  /*  @Before
    public void init() {
        props = new Properties();
        props.put("bootstrap.servers", "192.168.216.138:9092,192.168.216.139:9092,192.168.216.140:9092");
        props.put("group.id", "bfdConsumer");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        //props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    }*/

    public static void main(String[] args) throws Exception {
    	System.out.println("begin consumer");
    	 
    	InetAddress host = InetAddress.getLocalHost();
    	String hostName = host.getHostName();//获取计算机主机名 
    	Properties prop = new Properties();
 		InputStream input = new FileInputStream(args[0]);
 		prop.load(input);
 		
    	//初始化kafka consumer配置信息
         props = new Properties();
         props.put("bootstrap.servers",prop.getProperty("bootstrap.servers"));
         //props.put("zookeeper.connect", "192.168.205.153:2181");//声明zk 
         props.put("group.id", prop.getProperty("group.id"));
         props.put("enable.auto.commit", prop.getProperty("enable.auto.commit"));
         props.put("auto.commit.interval.ms", prop.getProperty("auto.commit.interval.ms"));
         props.put("session.timeout.ms", prop.getProperty("session.timeout.ms"));
         props.put("auto.offset.reset", prop.getProperty("auto.offset.reset"));
         props.put("key.deserializer", prop.getProperty("key.deserializer"));
         //props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
         props.put("value.deserializer", prop.getProperty("value.deserializer"));
         
         
         
 		//String path = ConsumeTest.class.getResource("/china-region.json").getPath()
        
        //加载china-region映射文件
 		String path = prop.getProperty("china_region_file");
        RegionDataInput regionDataInput = new JSONFileRegionDataInput(path);
 		//System.out.println(path+"/"+"china-region.json");
 		// 创建并初始化位置解析引擎，一般配置为全局单例
 		engine = new LocationParserEngine(regionDataInput);
 		// 初始化，加载数据，比较耗时
 		try {
 			long begin = System.currentTimeMillis();
 			engine.init();
 			long end = System.currentTimeMillis();
 			System.out.println("加载地图耗时"+(end-begin)/1000);
 		} catch (ParserEngineException e) {
 			// TODO Auto-generated catch block
 			System.out.println("地图初始化异常...");
 			e.printStackTrace();
 		}
    	 //args[1]数据输出的路径
 		String outDir = prop.getProperty("data_output_path");
         connectionKafka(outDir,hostName);
         System.out.println("finish consumer");
	}

  //  @SuppressWarnings("resource")
    /***
     * 
     * @param path  输出路径
     * @param args  topic名称
     * @throws Exception
     */
    public static void connectionKafka(String path,String hostName) throws Exception {
    	
        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);
        //TRACK.UP.QUEUE.98  一汽物流	 12 partitions
        //TRACK.UP.QUEUE.99 解放前装  48 partitions
        consumer.subscribe(Arrays.asList("TRACK.UP.QUEUE.98", "TRACK.UP.QUEUE.99"));
        CarInfo carInfo = null;
        StringBuffer data = null;
        String today = "";//记录当天时间
        while (true) {
        	/* 读取数据，读取超时时间为10s */
            ConsumerRecords<String, byte[]> records = consumer.poll(1000*100);
            data = new StringBuffer(); //保存输出结果
            
            Date readTime = new Date();
            SimpleDateFormat simpleDayFormat = new SimpleDateFormat("yyyyMMdd");
            SimpleDateFormat simpleHourFormat = new SimpleDateFormat("yyyyMMddHH");
            String dayString = simpleDayFormat.format(readTime);
            String hourString = simpleHourFormat.format(readTime);
            //创建输出目录 
          	FileUtil.createDir(path+"/"+dayString);
            String filePath = path+"/"+dayString+"/"+"kafka_"+hourString+"_"+hostName;
            FileUtil.createFile(filePath);
          //输出文件流
        	BufferedWriter writer = new BufferedWriter(new FileWriter(filePath,true));
            
        	if(!records.isEmpty()){
	            for (ConsumerRecord<String, byte[]> record : records) {
	            	carInfo = new CarInfo();
	                //接收数据
	            	byte[] receive = record.value();
	                //反序列化，解析包
	                PackageHead packageHead = PackageHead.parseFrom(receive);
	                //用这个 T_0000_POSITION  过滤出来 就是 定位数据	
	                if(packageHead.getBMessageID().toStringUtf8().equals(ConsumerJob.T_0000_POSITION)){
		                ByteString messageBody = packageHead.getBMessageBody();
		                //解析包体
		                PositionInfoReport positionInfoReport = PositionInfoReport.parseFrom(messageBody);
		                
		                long deviceId = packageHead.getNDeviceID();//获取设备编号
		                //获取基础信息对象
		                PositionBasicInfo positionBaseInfo = positionInfoReport.getBasicInfo();
		                
		                long dateTime = positionBaseInfo.getDateTime();//获取时间
		                double lati = positionBaseInfo.getLati();//获取纬度
		                double longi = positionBaseInfo.getLongi();//获取经度
		                int status = positionBaseInfo.getStatus();//获取状态位
		                int speed = positionBaseInfo.getSpeed();//获取速度
		                
		                //高德经纬度转换为百度
		                // String region = l
		                //根据经纬度获取区域
		                // 执行解析操作
		                //System.out.println("deviceid:"+deviceId+"longi:"+longi+","+"lati:"+lati);
		                Location location = engine.parse(longi,lati);
		                if(location !=null){
			                // 获取省市区信息
			                /*String provInfo = (location.getProv()!=null?location.getProv().getName():"");
			                String cityInfo = (location.getCity()!=null?location.getCity().getName():"");
			                String districtInfo = (location.getDistrict()!=null?location.getDistrict().getName():"");*/
		                	String provInfo = (location.getProv()!=null?String.valueOf(location.getProv().getCode()):"");
			                String cityInfo = (location.getCity()!=null?String.valueOf(location.getCity().getCode()):"");
			                String districtCode = (location.getDistrict()!=null?String.valueOf(location.getDistrict().getCode()):"");
			                
			                //System.out.println(provInfo+":"+cityInfo+":"+districtInfo);
			                
			                //信息封装进对象
			                carInfo.setDeviceId(deviceId);
			                carInfo.setDateTime(dateTime);
			                carInfo.setLati(lati);
			                carInfo.setLongi(longi);
			                carInfo.setSpeed(speed);
			                carInfo.setStatus(status);
			                carInfo.setProvince(provInfo);
			                carInfo.setCity(cityInfo);
			                //carInfo.setRegion(districtInfo);
			                carInfo.setRegionCode(districtCode);
			                
			                //解决最后一行有回车换行的问题
			                if(new File(filePath).length()==0){
			                	writer.write(carInfo.toString());
			                	writer.flush();
			                	
			                }else{
			                	data = data.append("\r\n"+carInfo.toString());
			               }
		                }else{
		                	loscount++;
		                	continue;
		                }
	                }
	                
	            }
	            writer.write(data.toString());
	            writer.close();
        	}
        	System.out.println("未分析出的区域数量："+loscount);
        }
    }
}