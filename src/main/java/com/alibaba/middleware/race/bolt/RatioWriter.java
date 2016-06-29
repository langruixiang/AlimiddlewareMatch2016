package com.alibaba.middleware.race.bolt;

import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.Constants;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.jstorm.RaceTopology;
import com.alibaba.middleware.race.rocketmq.CounterFactory;
import com.alibaba.middleware.race.rocketmq.CounterFactory.DecoratorTreeMap;
import com.alibaba.middleware.race.util.DoubleUtil;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class RatioWriter implements IBasicBolt{
	private static final long serialVersionUID = -8998720475277834236L;

	private static Logger LOG = LoggerFactory.getLogger(RatioWriter.class);
	private transient TairOperatorImpl tairOperator;
	
	private DecoratorTreeMap PCSumCounter;
	private DecoratorTreeMap WirelessSumCounter;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		Long key = tuple.getLong(0);
		Double value = tuple.getDouble(1);
		if (value < Constants.DOUBLE_DIFF_THREHOLD) {
		    return;
		}
		
		if(tuple.getSourceComponent().equals(RaceTopology.PCSUMCOUNTERRBOLT)){
			PCSumCounter.put(key, PCSumCounter.get(key) + value);			
		}else{
			WirelessSumCounter.put(key, WirelessSumCounter.get(key) + value);
		}
		
		Double pcSum = 0.0;
		Double wirelessSum = 0.0;
		
		for(Map.Entry<Long, Double> entry : PCSumCounter.entrySet()){
			Long entryKey = entry.getKey();
			
			pcSum += PCSumCounter.get(entryKey);
			wirelessSum += WirelessSumCounter.get(entryKey);
			
			if(pcSum > 1e-6){
				double ratio = wirelessSum / pcSum;
				tairOperator.write(RaceConfig.prex_ratio + entryKey, DoubleUtil.roundedTo2Digit(ratio));
				LOG.info("Ratio Writer:" + entryKey + ":" + WirelessSumCounter.get(entryKey) / PCSumCounter.get(entryKey));
			}
			
		}
	}
		
		

	@Override
	public void prepare(Map arg0, TopologyContext arg1) {
		// TODO Auto-generated method stub
		tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
                RaceConfig.TairGroup, RaceConfig.TairNamespace);
		
		PCSumCounter = CounterFactory.createTreeCounter();
		WirelessSumCounter = CounterFactory.createTreeCounter();
		
//		for(Map.Entry<Long, Double> entry : PCSumCounter.entrySet()){
//			Long key = entry.getKey();
//			tairOperator.write(RaceConfig.prex_ratio + key, 0.0);
//		}
	}

}
