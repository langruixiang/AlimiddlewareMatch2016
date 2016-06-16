package com.alibaba.middleware.race.spout;

import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.PaymentMessage;
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
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class PaySpout implements IRichSpout {

	private static Logger LOG = LoggerFactory.getLogger(PaySpout.class);
	
	private SpoutOutputCollector _collector;
	    
	private transient DefaultMQPushConsumer consumer;
	private transient LinkedBlockingQueue<PaymentMessage> messageQueue;

	private void initConsumer() throws MQClientException{
		consumer = new DefaultMQPushConsumer("PayConsumer");
		messageQueue = new LinkedBlockingQueue<PaymentMessage>();
		
   	    consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setNamesrvAddr(RaceConfig.MQNameServerAddr);
        try {
			consumer.subscribe(RaceConfig.MqPayTopic, "*");
		} catch (MQClientException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
        consumer.registerMessageListener(new MessageListenerConcurrently() {

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {
                try {
					for (MessageExt msg : msgs) {

					     byte [] body = msg.getBody();
					     if (body.length == 2 && body[0] == 0 && body[1] == 0) {
					         System.out.println("Got the end signal");
					         continue;
					     }
					     
					     PaymentMessage paymentMessage = RaceUtils.readKryoObject(PaymentMessage.class, body);
					     messageQueue.put(paymentMessage);
					 }
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
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
			PaymentMessage paymentMessage = messageQueue.take();
			Values values = new Values(paymentMessage.getOrderId(), paymentMessage.getCreateTime(), paymentMessage.getPayAmount(),
					paymentMessage.getPayPlatform(), paymentMessage.getPaySource());
			_collector.emit(values, values);
			
			LOG.info("PaySpout Emit:" + paymentMessage.toString());
			
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Override
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector collector) {
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
		declarer.declare(new Fields("orderID", "createTime", "payAmount", "platForm", "source"));
	}
	
	@Override
    public void ack(Object msgId) {
        
    }

    @Override
    public void fail(Object msgId) {
        _collector.emit((Values)msgId, msgId);
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
