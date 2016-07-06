package com.alibaba.middleware.race.rocketmq;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;


/**
 * create DefaultMQPushConsumer
 */

public class ConsumerFactory {

    private ConsumerFactory(){};

    public static DefaultMQPushConsumer create(MessageListenerConcurrently listener, String consumerGroup, String... topicNames) throws MQClientException {
        DefaultMQPushConsumer ret = new DefaultMQPushConsumer(consumerGroup);
        ret.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
//        ret.setNamesrvAddr(RaceConfig.MQNameServer);
        for (String topic : topicNames) {
            ret.subscribe(topic, "*");
        }
        ret.registerMessageListener(listener);
        return ret;
    }
    
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
