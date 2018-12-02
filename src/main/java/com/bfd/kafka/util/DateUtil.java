package com.bfd.kafka.util;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtil {
	public static String getToday(){
		Date date = new Date();
		SimpleDateFormat sdFormat = new SimpleDateFormat("yyyyMMdd");
		String dateString = sdFormat.format(date);
		return dateString;
	}

}
