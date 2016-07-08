package com.alibaba.middleware.race.rocketmq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;


/**
 * create DefaultMQPushConsumer
 */

public class ConsumerFactory {
	private static Logger LOG = LoggerFactory.getLogger(CounterFactory.class);	

    private ConsumerFactory(){};
    
    private static DefaultMQPushConsumer consumer;

    public static synchronized DefaultMQPushConsumer create(MessageListenerConcurrently listener, String consumerGroup, String... topicNames) throws MQClientException {
    	if(consumer != null){
    		LOG.info("Consumer has been created");
    		return null;
    	}    	
    	
    	consumer = new DefaultMQPushConsumer(consumerGroup);
    	consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
//        ret.setNamesrvAddr(RaceConfig.MQNameServer);
        for (String topic : topicNames) {
            consumer.subscribe(topic, "*");
        }
        consumer.registerMessageListener(listener);
        consumer.setPullBatchSize(RaceConfig.MQBatchSize);
        
        consumer.start();
        return consumer;
    }
    
    
    //Do not use in spout
    public static DefaultMQPushConsumer create(String consumerGroup, String... topicNames) throws MQClientException {
        DefaultMQPushConsumer ret = new DefaultMQPushConsumer(consumerGroup);
        ret.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
//        ret.setNamesrvAddr(RaceConfig.MQNameServer);
        for (String topic : topicNames) {
            ret.subscribe(topic, "*");
        }
        return ret;
    }
}
