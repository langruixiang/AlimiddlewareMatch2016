package com.alibaba.middleware.race.spout;

import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.bolt.TBMinuteCounter;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class TMTradeSpout implements IRichSpout {

	private static Logger LOG = LoggerFactory.getLogger(TMTradeSpout.class);
	
    private SpoutOutputCollector _collector;
    
    private transient DefaultMQPushConsumer consumer;
    private transient LinkedBlockingQueue<OrderMessage> messageQueue;
    
    private void initConsumer() throws MQClientException{
    	 consumer = new DefaultMQPushConsumer("TMTradeConsumer");
    	 messageQueue = new LinkedBlockingQueue<OrderMessage>();
    	 
    	 consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
         consumer.setNamesrvAddr(RaceConfig.MQNameServerAddr);
		 consumer.subscribe(RaceConfig.MqTmallTradeTopic, "*");
         consumer.registerMessageListener(new MessageListenerConcurrently() {

             @Override
             public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                             ConsumeConcurrentlyContext context) {
				for (MessageExt msg : msgs) {

				     byte [] body = msg.getBody();
				     if (body.length == 2 && body[0] == 0 && body[1] == 0) {
				         System.out.println("Got the end signal");
				         continue;
				     }
				     
				     OrderMessage orderMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
				     try {
						messageQueue.put(orderMessage);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				 }
                 return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
             }
         });

		 consumer.start();
    }

	
	
	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		try {
			OrderMessage orderMessage = messageQueue.take();
			long orderId = orderMessage.getOrderId();
			_collector.emit(new Values(orderId), orderId);
			
			LOG.info("TMTradeSpout Emit:" + orderMessage.toString());
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		_collector = collector;
		try {
			this.initConsumer();
		} catch (MQClientException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("orderID"));
	}
	
	@Override
    public void ack(Object msgId) {
        
    }

    @Override
    public void fail(Object msgId) {
        _collector.emit(new Values(msgId), msgId);
    }



	@Override
	public void activate() {
		// TODO Auto-generated method stub
		
	}



	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}



	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		
	}



	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
