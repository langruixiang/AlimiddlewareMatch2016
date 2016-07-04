package com.alibaba.middleware.race.bolt;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.middleware.race.Constants;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.jstorm.RaceTopology;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.middleware.race.model.PaymentMessageExt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class PlatformDistinguish implements IRichBolt, Runnable {
	private static Logger LOG = LoggerFactory.getLogger(PlatformDistinguish.class);
	
	private static final long serialVersionUID = -8918483233950498761L;
	private OutputCollector _collector;
	
    private long DEBUG_solveFailedCount = 0;
	private int _sendNumPerNexttuple = RaceConfig.DEFAULT_SEND_NUMBER_PER_NEXT_TUPLE;
	
	private transient LinkedBlockingQueue<PaymentMessageExt> solvedPayMessageQueue;//已经有salerPlatform的信息
    private transient LinkedBlockingQueue<PaymentMessageExt> unSolvedPayMessageQueue;//还没有salerPlatform的信息
    
    private transient ConcurrentHashMap<Long, Double> TMTradeMessage;
    private transient ConcurrentHashMap<Long, Double> TBTradeMessage;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declare) {
		// TODO Auto-generated method stub
		declare.declareStream(RaceTopology.TMPAYSTREAM, new Fields("TMPayMessage"));
        declare.declareStream(RaceTopology.TBPAYSTREAM, new Fields("TBPayMessage"));
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
	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub
		if(tuple.getSourceStreamId().equals(RaceTopology.PAYMENTSTREAM)){
			PaymentMessage paymentMessage = (PaymentMessage) tuple.getValue(1);			
			PaymentMessageExt orderMessage = new PaymentMessageExt(paymentMessage);
			solvePaymentMessageExt(orderMessage);	
			_collector.ack(tuple);
			
		}else if(tuple.getSourceStreamId().equals(RaceTopology.TMTRADESTREAM)){
			OrderMessage orderMessage = (OrderMessage) tuple.getValue(1);			
			Long orderID = orderMessage.getOrderId();
			Double price = orderMessage.getTotalPrice();
			TMTradeMessage.put(orderID, price);	
			_collector.ack(tuple);
		}else if(tuple.getSourceStreamId().equals(RaceTopology.TBTRADESTREAM)){
			OrderMessage orderMessage = (OrderMessage) tuple.getValue(1);			
			Long orderID = orderMessage.getOrderId();
			Double price = orderMessage.getTotalPrice();
			TBTradeMessage.put(orderID, price);	
			_collector.ack(tuple);
		}
		
		for (int i = 0; i < _sendNumPerNexttuple; ++i) {
            if(!solvedPayMessageQueue.isEmpty()){
                try {
                    PaymentMessageExt paymentMessageExt = solvedPayMessageQueue.take();
                    sendSolvedPayMentmessageExt(paymentMessageExt);

                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }

	}

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this._collector = collector;
		_sendNumPerNexttuple = JStormUtils.parseInt(
                conf.get("send.num.each.time"), RaceConfig.DEFAULT_SEND_NUMBER_PER_NEXT_TUPLE);
		
		solvedPayMessageQueue = new LinkedBlockingQueue<PaymentMessageExt>();
        unSolvedPayMessageQueue = new LinkedBlockingQueue<PaymentMessageExt>();
        
        TMTradeMessage = new ConcurrentHashMap<Long, Double>(RaceConfig.MapInitCapacity);      
        
        TBTradeMessage = new ConcurrentHashMap<Long, Double>(RaceConfig.MapInitCapacity);
        
        new Thread(this, "PlatformDistinguish").start();
	}
	
	 private boolean solvePaymentMessageExt(PaymentMessageExt paymentMessageExt) {
	        boolean ret = false;
	        try {
	            long orderId = paymentMessageExt.getOrderId();
	            if (TBTradeMessage.containsKey(orderId)){
	                paymentMessageExt.setSalerPlatform(Constants.TAOBAO);
	                solvedPayMessageQueue.put(paymentMessageExt);

	                //update order
	                Double lastAmount = TBTradeMessage.get(orderId);
	                if(lastAmount - paymentMessageExt.getPayAmount() < 1e-6){
	                    TBTradeMessage.remove(orderId);
	                }else{
	                    TBTradeMessage.put(orderId, lastAmount - paymentMessageExt.getPayAmount());
	                }

	                ret = true;
	            } else if (TMTradeMessage.containsKey(orderId)) {
	                paymentMessageExt.setSalerPlatform(Constants.TMALL);
	                solvedPayMessageQueue.put(paymentMessageExt);
	                
	                //update order
	                Double lastAmount = TMTradeMessage.get(orderId);
	                if(lastAmount - paymentMessageExt.getPayAmount() < 1e-6){
	                    TMTradeMessage.remove(orderId);
	                }else{
	                    TMTradeMessage.put(orderId, lastAmount - paymentMessageExt.getPayAmount());
	                }
	                
	                ret = true;
	            } else {
	                unSolvedPayMessageQueue.put(paymentMessageExt);
	                ret = false;
	            }
	        } catch (InterruptedException e) {
	            e.printStackTrace();
	        }
	        return ret;
	}
	 
	 private void sendSolvedPayMentmessageExt(PaymentMessageExt solvedPaymentMessageExt){
        Values values = new Values(solvedPaymentMessageExt);
        if (solvedPaymentMessageExt.isSalerPlatformTB()) {
            _collector.emit(RaceTopology.TBPAYSTREAM, values);	            
            LOG.info("PlatformDistinguish Emit TBPayment" + ":" + solvedPaymentMessageExt.toString());
        } else {
            _collector.emit(RaceTopology.TMPAYSTREAM, values);	            
            LOG.info("PlatformDistinguish Emit TMPayment" + ":" + solvedPaymentMessageExt.toString());
        }
	} 

	@Override
	public void run() {
		// TODO Auto-generated method stub		
		for (int i = 0; i < unSolvedPayMessageQueue.size(); ++i) {
            PaymentMessageExt paymentMessageExt = unSolvedPayMessageQueue.poll();//TODO change to take
            if (paymentMessageExt != null) {
                if(!solvePaymentMessageExt(paymentMessageExt)) {
                    ++DEBUG_solveFailedCount;
                    if (DEBUG_solveFailedCount > 2000000) {//TODO
                        LOG.info("DEBUG_solveFailed" + ":" + paymentMessageExt.toString());
                    }
                }
            }
        }
        if (unSolvedPayMessageQueue.isEmpty()) {
            JStormUtils.sleepMs(2000);//TODO
            LOG.info("sleeping...");
        } else {
            JStormUtils.sleepMs(10);//TODO
        }
	}

}
