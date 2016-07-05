package com.alibaba.middleware.race.bolt;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.Constants;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.rocketmq.CounterFactory;
import com.alibaba.middleware.race.rocketmq.CounterFactory.DecoratorHashMap;
import com.alibaba.middleware.race.util.DoubleUtil;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import io.netty.util.internal.ConcurrentSet;

public class TMCounterWriter implements IBasicBolt, Runnable{
	private static final long serialVersionUID = 6838822521222006295L;

	private static Logger LOG = LoggerFactory.getLogger(TMCounterWriter.class);
	
	private transient TairOperatorImpl tairOperator;
	private DecoratorHashMap sum;
	private Set<Long> receiveSet;
	
	private long TMWriterInterval = 2000L;
	
	private void writeTMCounter(){
		for(Long key : receiveSet){
			tairOperator.write(RaceConfig.prex_tmall + key, DoubleUtil.roundedTo2Digit(sum.get(key)));
			LOG.info("TMCounterWriter: " + RaceConfig.prex_tmall +  key + " " + sum.get(key));
		}
		
		receiveSet.clear();
	}

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
		Long key = tuple.getLong(0);
		Double value = tuple.getDouble(1);
        if (value < Constants.DOUBLE_DIFF_THREHOLD) {
            return;
        }

        sum.put(key, sum.get(key) + value);
        receiveSet.add(key);
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1) {
		// TODO Auto-generated method stub
		tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
                RaceConfig.TairGroup, RaceConfig.TairNamespace);
		
		sum = CounterFactory.createHashCounter();
		receiveSet = new ConcurrentSet<Long>();
		
		new Thread(this, "TMCounterWriter").start();
		
//		for(Map.Entry<Long, Double> entry : sum.entrySet()){
//			tairOperator.write(RaceConfig.prex_tmall + entry.getKey(), 0.0);
//		}
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		while(true){
			try {
				Thread.sleep(TMWriterInterval);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			writeTMCounter();
		}
	}

}
