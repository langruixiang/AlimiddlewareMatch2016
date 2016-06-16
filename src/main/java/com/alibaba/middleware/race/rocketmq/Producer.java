
package com.alibaba.middleware.race.rocketmq;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendCallback;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.middleware.race.model.*;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.bolt.PCSumCounter;

import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.Semaphore;


public class Producer {

    private static Random rand = new Random();
    private static int count = 1000;
    
    private static TreeMap<Long, Double> tmCounter = CounterFactory.createTreeCounter();
    private static TreeMap<Long, Double> tbCounter = CounterFactory.createTreeCounter();
    
    private static TreeMap<Long, Double> PCCounter = CounterFactory.createTreeCounter();
    private static TreeMap<Long, Double> WirelessCounter = CounterFactory.createTreeCounter();

    public static void main(String[] args) throws MQClientException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("producer");

        producer.setNamesrvAddr(RaceConfig.MQNameServerAddr);

        producer.start();

        final String [] topics = new String[]{RaceConfig.MqTaobaoTradeTopic, RaceConfig.MqTmallTradeTopic};
        final Semaphore semaphore = new Semaphore(0);

        for (int i = 0; i < count; i++) {
            try {
                final int platform = rand.nextInt(2);
                final OrderMessage orderMessage = ( platform == 0 ? OrderMessage.createTbaoMessage() : OrderMessage.createTmallMessage());
                orderMessage.setCreateTime(CounterFactory.startTimeStamp * 1000 + System.currentTimeMillis() % 86400000);

                byte [] body = RaceUtils.writeKryoObject(orderMessage);

                Message msgToBroker = new Message(topics[platform], body);

                producer.send(msgToBroker, new SendCallback() {
                    public void onSuccess(SendResult sendResult) {
                        System.out.println(orderMessage);
                        semaphore.release();
                    }
                    public void onException(Throwable throwable) {
                        throwable.printStackTrace();
                    }
                });

                //Send Pay message
                PaymentMessage[] paymentMessages = PaymentMessage.createPayMentMsg(orderMessage);
                double amount = 0;
                for (final PaymentMessage paymentMessage : paymentMessages) {
                    int retVal = Double.compare(paymentMessage.getPayAmount(), 0);
                    if (retVal < 0) {
                        throw new RuntimeException("price < 0 !!!!!!!!");
                    }

                    if (retVal > 0) {
                        amount += paymentMessage.getPayAmount();
                        final Message messageToBroker = new Message(RaceConfig.MqPayTopic, RaceUtils.writeKryoObject(paymentMessage));
                        producer.send(messageToBroker, new SendCallback() {
                            public void onSuccess(SendResult sendResult) {
                                System.out.println(paymentMessage);
                            }
                            public void onException(Throwable throwable) {
                                throwable.printStackTrace();
                            }
                        });
                        if(paymentMessage.getPayPlatform() == RaceConfig.PC){
                        	Long key = paymentMessage.getCreateTime() / 1000 / 60 * 60;
                        	PCCounter.put(key, PCCounter.get(key) + paymentMessage.getPayAmount());
                        }else{
                        	Long key = paymentMessage.getCreateTime() / 1000 / 60 * 60;
                        	WirelessCounter.put(key, WirelessCounter.get(key) + paymentMessage.getPayAmount());
                        }
                        
                    }else {
                        //
                    }
                }

                if (Double.compare(amount, orderMessage.getTotalPrice()) != 0) {
                    throw new RuntimeException("totalprice is not equal.");
                }


            } catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(2000);
            }
        }

        semaphore.acquire(count);

        byte [] zero = new  byte[]{0,0};
        Message endMsgTB = new Message(RaceConfig.MqTaobaoTradeTopic, zero);
        Message endMsgTM = new Message(RaceConfig.MqTmallTradeTopic, zero);
        Message endMsgPay = new Message(RaceConfig.MqPayTopic, zero);

        try {
            producer.send(endMsgTB);
            producer.send(endMsgTM);
            producer.send(endMsgPay);
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        for(Map.Entry<Long, Double> entry : PCCounter.entrySet()){
        	Long key = entry.getKey();
        	if(PCCounter.get(key) != 0 && WirelessCounter.get(key) != 0){
        		System.out.println(WirelessCounter.get(key) / PCCounter.get(key));
        	}
        }
        producer.shutdown();
    }
}
