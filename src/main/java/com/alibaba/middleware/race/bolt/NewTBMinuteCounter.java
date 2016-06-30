package com.alibaba.middleware.race.bolt;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.jstorm.RaceTopology;
import com.alibaba.middleware.race.rocketmq.CounterFactory;
import com.alibaba.middleware.race.rocketmq.CounterFactory.DecoratorHashMap;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class NewTBMinuteCounter implements IRichBolt, Runnable{

    private static final long serialVersionUID = 4732558042278288569L;
    private OutputCollector _collector = null;
    private static final long SEND_TUPLES_INTERVAL = 2000;

    private static Logger LOG = LoggerFactory.getLogger(NewTBMinuteCounter.class);
	private long lastSendTime = 0;
	
	private DecoratorHashMap PCCounter;
	private DecoratorHashMap WirelessCounter;

	private transient LinkedBlockingQueue<Tuple> _inputTuples;
	
	private int counter = 0;

    @Override
    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector) {
        this._collector = collector;
        this._inputTuples = new LinkedBlockingQueue<Tuple>();
        this.PCCounter = CounterFactory.createHashCounter();
        this.WirelessCounter = CounterFactory.createHashCounter();
        new Thread(this, "NewTBMinuteCounterProcessTuples").start();
    }

    @Override
	public void execute(Tuple input) {
		LOG.info("TBMinute Counter Receive" + ++counter + input.toString());
		try {
	        _inputTuples.put(input);
            _collector.ack(input);//TODO
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
	}

    @Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(RaceTopology.TBPCCOUNTERSTREAM, new Fields("key", "value"));
		declarer.declareStream(RaceTopology.TBWIRELESSSTREAM, new Fields("key", "value"));
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
	
	public void sendTuples() {
	    for(Entry<Long, Double> entry : PCCounter.entrySet()){
            if(entry.getValue() - 0 > 1e-6){
                _collector.emit(RaceTopology.TBPCCOUNTERSTREAM, new Values(entry.getKey(), entry.getValue()));//TODO add anchor
                LOG.info("TBMinuteCounter Emit PCCounter" + entry.getKey() + " : " + entry.getValue());
            }
        }
        CounterFactory.cleanCounter(PCCounter);
        for(Map.Entry<Long, Double> entry : WirelessCounter.entrySet()){
            if(entry.getValue() - 0 > 1e-6){
                _collector.emit(RaceTopology.TBWIRELESSSTREAM, new Values(entry.getKey(), entry.getValue()));//TODO add anchor
                LOG.info("TBMinuteCounter Emit WirelessCounter" + entry.getKey() + " : " + entry.getValue());
            }
        }
        CounterFactory.cleanCounter(WirelessCounter);
	}

    @Override
    public void run() {
        while (true) {
            Tuple tuple = _inputTuples.poll();
            while (tuple != null) {
                if(tuple.getSourceStreamId().equals(RaceTopology.TBPAYSTREAM)){
                    long createTime = tuple.getLong(1);
                    double payAmount = tuple.getDouble(2);
                    short payPlatform = tuple.getShort(3);

                    long timeStamp = (createTime / 1000 / 60) * 60;
                    if(payPlatform == RaceConfig.PC){
                        PCCounter.put(timeStamp, PCCounter.get(timeStamp) + payAmount);
                    }else{
                        WirelessCounter.put(timeStamp, WirelessCounter.get(timeStamp) + payAmount);
                    }
                }
                sendTuplesIfTimeIsUp();
                tuple = _inputTuples.poll();
            }
            sendTuplesIfTimeIsUp();
            JStormUtils.sleepMs(10);//TODO remove
        }
    }

    private void sendTuplesIfTimeIsUp() {
        if(System.currentTimeMillis() - lastSendTime >= SEND_TUPLES_INTERVAL){
            sendTuples();
            lastSendTime = System.currentTimeMillis();
        }
    }
}
