package com.alibaba.middleware.race.rocketmq;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.middleware.race.model.*;
import com.alibaba.middleware.race.RaceUtils;

import java.util.List;

public class Consumer {
	private static int counter = 0;

    public static void main(String[] args) throws InterruptedException, MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(RaceConfig.MetaConsumerGroup + "pay");

        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        
//        consumer.setNamesrvAddr(RaceConfig.MQNameServerAddr);
        System.out.println(consumer.getNamesrvAddr());

        consumer.subscribe(RaceConfig.MqPayTopic, "*");

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

                    PaymentMessage paymentMessage = RaceUtils.readKryoObject(PaymentMessage.class, body);
                    counter++;
                    System.out.println("" + counter + paymentMessage);
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer.start();
        
        DefaultMQPushConsumer consumer1 = new DefaultMQPushConsumer(RaceConfig.MetaConsumerGroup + "TBTrade");

        consumer1.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        
//        consumer1.setNamesrvAddr(RaceConfig.MQNameServerAddr);

        consumer1.subscribe(RaceConfig.MqTaobaoTradeTopic, "*");

        consumer1.registerMessageListener(new MessageListenerConcurrently() {

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {

                    byte [] body = msg.getBody();
                    if (body.length == 2 && body[0] == 0 && body[1] == 0) {
                        System.out.println("Got the end signal");
                        continue;
                    }

                    OrderMessage orderMessage= RaceUtils.readKryoObject(OrderMessage.class, body);
                    counter++;
                    System.out.println("" + counter + orderMessage);
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer1.start();
        
        DefaultMQPushConsumer consumer2 = new DefaultMQPushConsumer(RaceConfig.MetaConsumerGroup + "TMTrade");

        consumer2.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        
//        consumer2.setNamesrvAddr(RaceConfig.MQNameServerAddr);

        consumer2.subscribe(RaceConfig.MqTmallTradeTopic, "*");

        consumer2.registerMessageListener(new MessageListenerConcurrently() {

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {

                    byte [] body = msg.getBody();
                    if (body.length == 2 && body[0] == 0 && body[1] == 0) {
                        System.out.println("Got the end signal");
                        continue;
                    }

                    OrderMessage orderMessage= RaceUtils.readKryoObject(OrderMessage.class, body);
                    counter++;
                    System.out.println("" + counter + orderMessage);
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        consumer2.start();

        System.out.println("Consumer Started.");
    }
}
