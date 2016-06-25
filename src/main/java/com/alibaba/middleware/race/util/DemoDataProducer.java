package com.alibaba.middleware.race.util;

import com.alibaba.middleware.race.Constants;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendCallback;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.middleware.race.model.*;
import com.alibaba.middleware.race.rocketmq.CounterFactory;
import com.alibaba.middleware.race.util.FileUtil;
import com.alibaba.middleware.race.RaceUtils;

import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.Semaphore;

public class DemoDataProducer {

    private static Random rand = new Random();
    private static int count = 1000;

    private static TreeMap<Long, Double> tmCounter = CounterFactory
            .createTreeCounter();
    private static TreeMap<Long, Double> tbCounter = CounterFactory
            .createTreeCounter();

    private static TreeMap<Long, Double> PCCounter = CounterFactory
            .createTreeCounter();
    private static TreeMap<Long, Double> WirelessCounter = CounterFactory
            .createTreeCounter();

    private static int paymentCounter = 0;

    public static void main(String[] args) throws MQClientException,
            InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer(
                RaceConfig.MetaConsumerGroup + "producer");

//        producer.setNamesrvAddr(RaceConfig.MQNameServerAddr);
        producer.setSendMsgTimeout(5000);

        producer.start();

        final String[] topics = new String[] { RaceConfig.MqTaobaoTradeTopic,
                RaceConfig.MqTmallTradeTopic };
        final Semaphore semaphore = new Semaphore(0);

        for (int i = 0; i < count; i++) {
            try {
                // Send Order message
                final int platform = rand.nextInt(2);
                final OrderMessage orderMessage = (platform == 0 ? OrderMessage
                        .createTbaoMessage() : OrderMessage
                        .createTmallMessage());
                orderMessage.setCreateTime(CounterFactory.startTimeStamp * 1000
                        + System.currentTimeMillis() % 86400000);

                byte[] body = RaceUtils.writeKryoObject(orderMessage);

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

                // Send Pay message
                PaymentMessage[] paymentMessages = PaymentMessage
                        .createPayMentMsg(orderMessage);
                double amount = 0;
                for (final PaymentMessage paymentMessage : paymentMessages) {
                    int retVal = Double.compare(paymentMessage.getPayAmount(),
                            0);
                    if (retVal < 0) {
                        throw new RuntimeException("price < 0 !!!!!!!!");
                    } else if (retVal > 0) {
                        paymentCounter++;
                        amount += paymentMessage.getPayAmount();
                        final Message messageToBroker = new Message(
                                RaceConfig.MqPayTopic,
                                RaceUtils.writeKryoObject(paymentMessage));
                        producer.send(messageToBroker, new SendCallback() {
                            public void onSuccess(SendResult sendResult) {
                                System.out.println(paymentMessage);
                            }

                            public void onException(Throwable throwable) {
                                throwable.printStackTrace();
                            }
                        });

                        Long key = paymentMessage.getCreateTime() / 1000 / 60 * 60;
                        if (paymentMessage.getPayPlatform() == RaceConfig.PC) {
                            PCCounter.put(key, PCCounter.get(key)
                                    + paymentMessage.getPayAmount());
                        } else {
                            WirelessCounter.put(key, WirelessCounter.get(key)
                                    + paymentMessage.getPayAmount());
                        }

                        if (orderMessage.getSalerId().startsWith("tb")) {
                            tbCounter.put(key, tbCounter.get(key)
                                    + paymentMessage.getPayAmount());
                        } else {
                            tmCounter.put(key, tmCounter.get(key)
                                    + paymentMessage.getPayAmount());
                        }

                    } else {
                        // paymentMessage with amount equals 0
                    }
                }// for

                if (Double.compare(amount, orderMessage.getTotalPrice()) != 0) {
                    throw new RuntimeException("totalprice is not equal.");
                }

            } catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(2000);
            }
        }// for

        semaphore.acquire(count);

        byte[] zero = new byte[] { 0, 0 };
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

        // output taobao results
        for (Map.Entry<Long, Double> entry : tbCounter.entrySet()) {
            if (entry.getValue() > 0.0) {
                outputExpectedResult(RaceConfig.prex_taobao + entry.getKey(), entry.getValue());
            }
        }

        // output tmall results
        for (Map.Entry<Long, Double> entry : tmCounter.entrySet()) {
            if (entry.getValue() > 0.0) {
                outputExpectedResult(RaceConfig.prex_tmall + entry.getKey(), entry.getValue());
            }
        }

        // output ratio results
        Double pcSum = 0.0;
        Double wirelessSum = 0.0;
        for (Map.Entry<Long, Double> entry : PCCounter.entrySet()) {
            Long key = entry.getKey();
            pcSum += entry.getValue();
            wirelessSum += WirelessCounter.get(key);
            double ratio = 0.0;
            if (pcSum.compareTo(0.0) == 0) {
                ratio = -1;
            } else {
                ratio = wirelessSum / pcSum;
            }
            outputExpectedResult(RaceConfig.prex_ratio + entry.getKey(), ratio);
        }
        System.out.println("paymentCounter:" + paymentCounter);
        producer.shutdown();
    }
    
    private static void outputExpectedResult(String key, Double value) {
//      System.out.println(key + ":" + value);
      FileUtil.appendLineToFile(Constants.EXPECTED_RESULT_FILE, key + ":" + value);
  }
}
