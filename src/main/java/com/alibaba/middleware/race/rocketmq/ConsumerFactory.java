package com.alibaba.middleware.race.rocketmq;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;


/**
 * create DefaultMQPushConsumer
 */

public class ConsumerFactory {

    private ConsumerFactory(){};

    public static DefaultMQPushConsumer create(String consumerGroup, String... topicNames) throws InterruptedException, MQClientException {
        DefaultMQPushConsumer ret = new DefaultMQPushConsumer(consumerGroup);
        ret.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
//        ret.setNamesrvAddr(RaceConfig.MQNameServer);
        for (String topic : topicNames) {
            ret.subscribe(topic, "*");
        }
        return ret;
    }
}
