package com.bfd.kafka.multiprocess;

//import com.alibaba.fastjson.JSONObject;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import tech.spiro.addrparser.io.RegionDataInput;
import tech.spiro.addrparser.io.file.JSONFileRegionDataInput;
import tech.spiro.addrparser.parser.Location;
import tech.spiro.addrparser.parser.LocationParserEngine;
import tech.spiro.addrparser.parser.ParserEngineException;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.bfd.kafka.pojo.CarInfo;
import com.bfd.kafka.protobuf.JTT808PositionInfoReport.PositionInfoReport;
import com.bfd.kafka.protobuf.JTT808PositionInfoReport.PositionInfoReport.PositionBasicInfo;
import com.bfd.kafka.protobuf.QMDPPackageHead.PackageHead;
import com.bfd.kafka.test.TestDemo;
import com.bfd.kafka.util.FileUtil;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
 
/**
 *
 */
public class KafkaConsumerThread implements Runnable{
 
    private KafkaStream<byte[],byte[]> stream;
    private String topicNum;
    private CarInfo carInfo;
    private String 	path="";
    private static LocationParserEngine engine = null;
    static{
		String fileName = TestDemo.class.getClassLoader().getResource("china-region.json").getPath();
		RegionDataInput regionDataInput = new JSONFileRegionDataInput(fileName);
		// 创建并初始化位置解析引擎，一般配置为全局单例
		LocationParserEngine engine = new LocationParserEngine(regionDataInput);
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
    
    }
    public KafkaConsumerThread(KafkaStream<byte[],byte[]> stream, String topicNum) {
        this.stream = stream;
        this.topicNum = topicNum;
 
    }
    public void run() {
        ConsumerIterator<byte[],byte[]> it = stream.iterator();
        StringBuffer data = new StringBuffer(); //保存输出结果
        Date readTime = new Date();
        SimpleDateFormat simpleDayFormat = new SimpleDateFormat("yyyyMMdd");
        SimpleDateFormat simpleHourFormat = new SimpleDateFormat("yyyyMMddHH");
        String dayString = simpleDayFormat.format(readTime);
        String hourString = simpleHourFormat.format(readTime);
        //创建输出目录 
      	FileUtil.createDir(path+"/"+dayString);
        String filePath = path+"kafka_"+hourString;
        FileUtil.createFile(filePath);
      //输出文件流
    	//BufferedWriter writer = new BufferedWriter(new FileWriter(filePath,true));
    	
        while (it.hasNext()){
            if(KafkaConsumer.isStop()){
                flushData();
                break;
            }
            MessageAndMetadata<byte[],byte[]> record = it.next();
            /*JSONObject jsStr = JSONObject.parseObject(m.message());
            
            String jsStr="";
            //String dateTime = new SimpleDateFormat("yyyyMMddHH").format(jsStr.get("createTime"));
            String dateTime = "";*/
        	carInfo = new CarInfo();
            //接收数据
        	byte[] receive = record.message();
            //反序列化，解析包
            PackageHead packageHead=null;
			try {
				packageHead = PackageHead.parseFrom(receive);
			} catch (InvalidProtocolBufferException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            ByteString messageBody = packageHead.getBMessageBody();
            //解析包体
            PositionInfoReport positionInfoReport=null;
			try {
				positionInfoReport = PositionInfoReport.parseFrom(messageBody);
			} catch (InvalidProtocolBufferException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            
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
            Location location = engine.parse(longi,lati);

            // 获取省市区信息
            String provInfo = location.getProv().getName();
            String cityInfo = location.getCity().getName();
            String districtInfo = location.getDistrict().getName();
            System.out.println(provInfo+":"+cityInfo+":"+districtInfo);
            
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
            
            //解决最后一行有回车换行的问题
            if(new File(filePath).length()==0){
            	data = data.append(carInfo.toString());
            }else{
            	data = data.append("\r\n"+carInfo.toString());
            }  
        }
            FileWrapper writer = getFileByName(filePath);
            writer.write(data.toString());
        }
 
    static Map<String,FileWrapper> fileMap=new ConcurrentHashMap<>();
    FileWrapper getFileByName(String name){
        FileWrapper bufferedWriter = fileMap.get(name);
        if(bufferedWriter!=null){
            return bufferedWriter;
        }
        synchronized (fileMap){
            bufferedWriter=fileMap.get(name);
            if(bufferedWriter!=null){
                return bufferedWriter;
            }
            File file = new File(name);
            System.out.println("file.name==========="+file.getName());
            FileWrapper result = new FileWrapper(name);
            if(result==null){
                new Throwable("result为空！");
            }
            fileMap.put(name,result);
            return result;
        }
 
    }
    private static class FileWrapper {
        private final String name;
        private AtomicInteger calc = new AtomicInteger();
        private final BufferedWriter br;
        private long starttime=new Date().getTime();
        private FileOutputStream fos;
        private OutputStreamWriter osw;
 
        public FileWrapper(String name) {
            this.name=name;
            try {
                fos=new FileOutputStream(name,true);
                osw=new OutputStreamWriter(fos,"utf-8");
                this.br = new BufferedWriter(osw);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                throw new RuntimeException("文件不可用"+name,e);
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
                throw new RuntimeException("文件不可用"+name,e);
            }
        }
 
        public void write (String txt){
            try {
                //int i = calc.incrementAndGet();
                String txt2=txt+"\n";
                br.write(txt2);
                //if(i%10000==0){
                    br.flush();
                    //calc.set(0);
                //}
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
 
        public void close(){
            try {
                br.close();
            } catch (IOException e) {
            }
            try {
                osw.close();
            } catch (IOException e) {
            }
            try {
                fos.close();
                fos=null;
            } catch (IOException e) {
            }
        }
    }
 
 
    static void flushData(){
        long nowtime = new Date().getTime();
        synchronized (fileMap){
            for (FileWrapper fileWrapper : fileMap.values()) {
                //fileWrapper.br.flush();
                if(nowtime-fileWrapper.starttime>2*60*60*1000){
                    fileMap.remove(fileWrapper.name);
                    fileWrapper.close();
                }
            }
        }
    }
    static {
        Timer timer = new Timer("flush-task", false);
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                flushData();
            }
        },60000,60000);
    }
 
}

