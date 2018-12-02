package com.bfd.kafka.multiprocess;

//import com.alibaba.fastjson.JSONObject;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
//import lombok.extern.log4j.Log4j2;
 
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
 
 
//@Log4j2
public class KafkaConsumer implements Runnable{
 
    static boolean stop = false;
 
    public static boolean isStop() {
        return stop;
    }
    public static void setStop(boolean stop) {
        KafkaConsumer.stop = stop;
    }
 
    private static Thread mainThread = Thread.currentThread();
    public static Properties properties() throws IOException {
        Properties properties_kafkainfo=new Properties();
 
        InputStream in = new FileInputStream("/opt/logs/java_jar/kafkainfo.properties");
        properties_kafkainfo.load(in);
        return properties_kafkainfo;
    }
    public static Properties properties_topic() throws IOException {
        Properties properties_kafkatopic=new Properties();
        InputStream in = new FileInputStream("/opt/logs/java_jar/topic.properties");
        properties_kafkatopic.load(in);
        return properties_kafkatopic;
    }
 
    public final ConsumerConnector consumer;
    public KafkaConsumer() throws IOException {
        Properties props = new Properties();
        //props.put("zookeeper.connect", "10.20.30.91:2181,10.20.30.92:2181,10.20.30.93:2181");
        props.put("zookeeper.connect", properties().getProperty("zookeeper_connect"));
        props.put("group.id", properties().getProperty("group"));
        props.put("zookeeper.session.timeout.ms", properties().getProperty("session_timeout_ms"));
        props.put("zookeeper.sync.time.ms", properties().getProperty("zookeeper_sync_time_ms"));
        props.put("auto.commit.interval.ms", properties().getProperty("auto_commit_interval_ms"));
        props.put("auto.commit.enable",properties().getProperty("auto_commit_enable"));
        props.put("auto.offset.reset", properties().getProperty("auto_offset_reset")); //largest smallest
        //props.put("serializer.class", properties().getProperty("serializer_class"));
        ConsumerConfig config = new ConsumerConfig(props);
        consumer =   kafka.consumer.Consumer.createJavaConsumerConnector(config);
    }
    public void run() {
        final int numThreads = 6;
        final Iterator<String> topic;
        try {
            topic = properties_topic().stringPropertyNames().iterator();
            Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
            List<String> topicList = new ArrayList<String>();
            while(topic.hasNext()){
                final String key_topic = topic.next();
                topicList.add(key_topic);
                topicCountMap.put(key_topic, new Integer(1));
            }	
            StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
            //ByteDecoder valueDecoder = new StringDecoder(new VerifiableProperties());
            final Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap =
                    consumer.createMessageStreams(topicCountMap); //topic ,List为几个分区
            for(int i=0;i<topicList.size();i++) {
                final String key_topic1 = topicList.get(i);
                //for (int j = 0; j < 6; j++) {
                    new Thread(
                        new Runnable() {
                            @Override
                            public void run() {
                                List<KafkaStream<byte[], byte[]>> stream = consumerMap.get(key_topic1);
                                ExecutorService executor = Executors.newFixedThreadPool(numThreads);
                                for (KafkaStream streams : stream) {
                                    executor.submit(new KafkaConsumerThread(streams, key_topic1));
                                }
                            }
                        }).start();
                }
           // }
 
 
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) throws IOException {
        Thread t = new Thread(new KafkaConsumer());
        t.start();
        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                KafkaConsumer.setStop(true);
                try {
                    sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }
}

