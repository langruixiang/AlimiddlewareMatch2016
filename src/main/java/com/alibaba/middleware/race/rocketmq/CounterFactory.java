package com.alibaba.middleware.race.rocketmq;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Set;
import java.util.TreeMap;

public class CounterFactory {
	private static Logger LOG = LoggerFactory.getLogger(CounterFactory.class);	
	
	public static Long timeStamp = 1L;
	//Decorate the get operation with init value	
	public static class DecoratorHashMap{
		private HashMap<Long, Double> innerMap = new HashMap<Long, Double>();
		
		public DecoratorHashMap(){
			
		}
		
		public Double get(Long key){
			if(innerMap.containsKey(key)){
				return innerMap.get(key);
			}else{
				LOG.info("Decorator Set Add:" + key);
				innerMap.put(key, 0.0);
				return 0.0;
			}
		}
		
		public void put(Long key, Double value){
			innerMap.put(key, value);
		}
		
		public boolean containsKey(Long key){
			return innerMap.containsKey(key);
		}
		
		public Set<Entry<Long, Double>> entrySet(){
			return innerMap.entrySet();
		}
		
		public void clean(){
			for(Long key: innerMap.keySet()){
				innerMap.put(key, 0.0);
			}
		}
	}
	
	public static class DecoratorTreeMap{
		private TreeMap<Long, Double> innerMap = new TreeMap<Long, Double>();
		public DecoratorTreeMap(){
			
		}
		
		public Double get(Long key){
			if(innerMap.containsKey(key)){
				return innerMap.get(key);
			}else{
				LOG.info("Decorator Set Add:" + key);
				innerMap.put(key, 0.0);
				return 0.0;
			}
		}
		
		public void put(Long key, Double value){
			innerMap.put(key, value);
		}
		
		public boolean containsKey(Long key){
			return innerMap.containsKey(key);
		}
		
		public Set<Entry<Long, Double>> entrySet(){
			return innerMap.entrySet();
		} 
		
		public void clean(){
			for(Long key: innerMap.keySet()){
				innerMap.put(key, 0.0);
			}
		}
	}
	
	
	public static DecoratorHashMap createHashCounter(){
		DecoratorHashMap counter = new DecoratorHashMap();		
		return counter;
	}
	
	public static DecoratorTreeMap createTreeCounter(){
		DecoratorTreeMap counter = new DecoratorTreeMap();
		return counter;
	}
	
	public static void cleanCounter(DecoratorHashMap counter){
		counter.clean();
	}
	
	public static void cleanCounter(DecoratorTreeMap counter){
		counter.clean();
	}
}

