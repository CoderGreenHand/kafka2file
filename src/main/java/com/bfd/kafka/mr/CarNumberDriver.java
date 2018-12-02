package com.bfd.kafka.mr;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class CarNumberDriver {

	/*private static final String FILES_IN = " ";
	private static final String FILES_OUT = " ";*/
	private static final String USAGE = "hadoop jar kafkaToFile.jar com.bfd.kafka.CarNumberDriver ${inpath} ${outpath}";
	//需要一个输入路径，一个输出路径，时间为变量，脚本传入
	//例如 /user/hive/warehouse/table/l_date=${}  /user/jiaze.yuan/tmp/route/table_1/l_date=${}
	public static void main(String[] args) throws IOException,
	InterruptedException, ClassNotFoundException {

		String inPath = null;
		if (args.length != 2) {
			System.out.println(USAGE);
			return;
		}

		Configuration conf = new Configuration();
		//conf.set("recursion.depth", depth + "");
        Job job = Job.getInstance(conf);
		job.setJobName("CarCountJob");

		job.setMapperClass(CarNumberMapper.class);
		job.setReducerClass(CarNumberReducer.class);
		job.setJarByClass(CarNumberDriver.class);

		/*Properties prop = new Properties();
		InputStream input = new FileInputStream(args[0]);
		prop.load(input);*/
		
		//int reducerNumber = Integer.valueOf(prop.getProperty("hadoop.reduce.tasks"));
		job.setNumReduceTasks(8);
		//中间结果按日期存储
	/*	SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd");
		Date date=new Date();
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.add(Calendar.DATE, -1);
		date = calendar.getTime();
		String yesterday = sdf.format(date);*/
		
		//inPath = prop.getProperty("hdfs.input");//获取输入路径
		inPath = args[0];
		//String outPath = prop.getProperty("hdfs.tmp.output")+"/"+yesterday;//获取输出路径
		String outPath = args[1];
		//outPath = (outPath == null ? FILES_OUT : outPath);
		Path in = new Path(inPath);
		Path out = new Path(outPath);

		FileInputFormat.addInputPath(job, in);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(out)) {
			fs.delete(out, true);
		}

		FileOutputFormat.setOutputPath(job, out);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.waitForCompletion(true);
		if(job.isSuccessful()){
			System.out.println("This job has finished......");
			System.exit(0);
		}else{
			System.exit(1);
		}

	}
}
