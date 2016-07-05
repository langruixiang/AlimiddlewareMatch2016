package com.alibaba.middleware.race.bolt;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.Constants;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.jstorm.RaceTopology;
import com.alibaba.middleware.race.util.DoubleUtil;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class RatioWriter implements IBasicBolt {
    private static final long serialVersionUID = -8998720475277834236L;

    private static final long WRITE_TAIR_INTERVAL = 30000;

    private static Logger LOG = LoggerFactory.getLogger(RatioWriter.class);
    private transient TairOperatorImpl tairOperator;

    private static ConcurrentHashMap<Long, Double> pcSumCounter = new ConcurrentHashMap<Long, Double>();
    private static ConcurrentHashMap<Long, Double> wirelessSumCounter = new ConcurrentHashMap<Long, Double>();
    private Map<Long, Double> tairCache;

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

        if (tuple.getSourceStreamId().equals(RaceTopology.TMPCCOUNTERSTREAM)
                || tuple.getSourceStreamId().equals(RaceTopology.TBPCCOUNTERSTREAM)) {
            Double oldValue = pcSumCounter.get(key);
            if (oldValue == null) {
                pcSumCounter.put(key, value);
            } else {
                pcSumCounter.put(key, oldValue + value);
            }
        } else {
            Double oldValue = wirelessSumCounter.get(key);
            if (oldValue == null) {
                wirelessSumCounter.put(key, value);
            } else {
                wirelessSumCounter.put(key, oldValue + value);
            }
        }
    }

    private synchronized void writeTair(Map<Long, Double> pcSumMap, Map<Long, Double> wirelessSumMap) {
        List<Long> times = new LinkedList<Long>();
        Double pcSum = 0.00;
        Double wirelessSum = 0.00;
        Iterator<Entry<Long, Double>> iter = pcSumMap.entrySet().iterator();
        while (iter.hasNext()) {
            Entry<Long, Double> entry = iter.next();
            times.add(entry.getKey());
        }
        Collections.sort(times);
        for (Long time : times) {
            pcSum += pcSumMap.get(time);
            if (wirelessSumMap.get(time) != null) {
                wirelessSum += wirelessSumMap.get(time);
            }
            if (pcSum > 1e-6) {
                double ratio = wirelessSum / pcSum;
                if (tairCache.get(time) != null && tairCache.get(time) - ratio < 1e-6) {
                    continue;
                }
                if (tairOperator.write(RaceConfig.prex_ratio + time, DoubleUtil.roundedTo2Digit(ratio))) {
                    tairCache.put(time, ratio);
                }
            }
        }
    }

    @Override
    public void prepare(Map arg0, TopologyContext arg1) {
        // TODO Auto-generated method stub
        tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer,
                RaceConfig.TairSalveConfigServer, RaceConfig.TairGroup,
                RaceConfig.TairNamespace);

        tairCache = new HashMap<Long, Double>();

        new Thread() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(WRITE_TAIR_INTERVAL);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    writeTair(pcSumCounter, wirelessSumCounter);
                }
            }
        }.start();
    }

}
