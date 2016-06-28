package com.alibaba.middleware.race.rocketmq;

import java.util.Calendar;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeMap;

public class CounterFactory {
	
	public static Long[] timeStamp = new Long[24 * 60];
	public static long startTimeStamp;
	
	static {
		Calendar calendar = Calendar.getInstance();
		calendar.set(2016, Calendar.JUNE, 28, 0, 0);
		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MILLISECOND, 0);
		
		for(int i = 0; i < timeStamp.length; i++){
			timeStamp[i] = calendar.getTimeInMillis() / 1000 / 60 * 60;
			calendar.add(Calendar.MINUTE, 1);
		}
		
		startTimeStamp = timeStamp[0];
	}
	
	public static HashMap<Long, Double> createHashCounter(){
		HashMap<Long, Double> counter = new HashMap<Long, Double>();
		for(int i = 0; i < timeStamp.length; i++){
			counter.put(timeStamp[i], 0.0);
		}
		
		return counter;
	}
	
	public static TreeMap<Long, Double> createTreeCounter(){
		TreeMap<Long, Double> counter = new TreeMap<Long, Double>();
		for(int i = 0; i < timeStamp.length; i++){
			counter.put(timeStamp[i], 0.0);
		}
		return counter;
	}
	
	public static void cleanCounter(Map<Long, Double> counter){
		for(int i = 0; i < timeStamp.length; i++){
			counter.put(timeStamp[i], 0.0);
		}
	}
}

