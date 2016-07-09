package com.alibaba.middleware.race.jstorm;

import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.middleware.race.Constants;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.MetaMessage;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.middleware.race.rocketmq.ConsumerFactory;
import com.alibaba.middleware.race.util.FileUtil;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class AllSpout implements IRichSpout, MessageListenerConcurrently {

	private static final long serialVersionUID = -8949381451255846180L;
	
	private static Logger LOG = LoggerFactory.getLogger(AllSpout.class);
	private SpoutOutputCollector _collector;

	private int _sendNumPerNexttuple = Constants.DEFAULT_SEND_NUMBER_PER_NEXT_TUPLE;

//    private static final boolean DEBUG_ENABLE = true;//TODO just for debug
//    private AtomicInteger DEBUG_receivedMsgCount = new AtomicInteger(0);
//    private AtomicInteger DEBUG_amountEqualsZeroPaymentMsgCount = new AtomicInteger(0);
//    private AtomicInteger DEBUG_sendTupleCount = new AtomicInteger(0);
//    private String DEBUG_thisSpoutName;
//    private long DEBUG_resendCount = 0;
	
    private AtomicBoolean _paymentMsgEndSignal = new AtomicBoolean(false);//TODO
    private AtomicLong _latestMsgArrivedTime = new AtomicLong(0);
    private static final long CONSUMER_MAX_WAITING_TIME = 1 * 60 * 1000;//此时间内收不到任何消息，且_paymentMsgEndSignal为true,则认为所有消息接收完成
    
    private transient LinkedBlockingQueue<MetaMessage> sendingQueue;
	
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
//        if (DEBUG_ENABLE) {
//            DEBUG_thisSpoutName = Thread.currentThread().getName();
//        }
        _collector = collector;
        sendingQueue = new LinkedBlockingQueue<MetaMessage>();
        initConsumer();
        
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("orderID", "metaMessage"));
    }
    
    @Override
    public void fail(Object msgId) {
//        _collector.emit(new Values(msgId), msgId);//TODO
//        ++DEBUG_resendCount;
    }

    private void initConsumer() {
        try {
            DefaultMQPushConsumer consumer = ConsumerFactory.create(
                    this,
                    RaceConfig.MetaConsumerGroup,
                    RaceConfig.MqTmallTradeTopic, 
                    RaceConfig.MqPayTopic,
                    RaceConfig.MqTaobaoTradeTopic);
//            if (DEBUG_ENABLE) {
//              if(consumer == null){
//                  FileUtil.appendLineToFile("/home/admin/consumer.txt", DEBUG_thisSpoutName + "Consumer already exist consumer in current worker, don't need to fetch data!");
//              } else {
//                  FileUtil.appendLineToFile("/home/admin/consumer.txt", DEBUG_thisSpoutName + "create consumer successfully!");
//              }
//            }
        } catch (MQClientException e) {
            e.printStackTrace();
            LOG.error("Failed in initConsumer", e);
            throw new RuntimeException("Failed in initConsumer", e);
        }
    }

    @Override
	public void nextTuple() {
        for (int i = 0; i < _sendNumPerNexttuple && !sendingQueue.isEmpty(); ++i) {
            MetaMessage metaTuple = sendingQueue.poll();
            if (metaTuple != null) {
//                if (DEBUG_ENABLE) {
//                  int tmpCount = DEBUG_sendTupleCount.addAndGet(1);
//                  FileUtil.appendLineToFile("/home/admin/send.txt", DEBUG_thisSpoutName + ":DEBUG_sendTupleCount " + tmpCount);
//                  FileUtil.appendLineToFile("/home/admin/detail_tuples.txt", DEBUG_thisSpoutName + " : " + metaTuple.toString());
//                  FileUtil.appendLineToFile("/home/admin/tuples.txt", metaTuple.toString());
//                }

            _collector.emit(new Values(metaTuple.getOrderId(), metaTuple));
            }
        }

	    long current = System.currentTimeMillis();
        if (_paymentMsgEndSignal.get() && sendingQueue.isEmpty()
                && current - _latestMsgArrivedTime.get() > CONSUMER_MAX_WAITING_TIME) {
//            sendEndSignals();
//            logDebugInfo();
            JStormUtils.sleepMs(2000);
        }
	}
	

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext context) {
    	
        try {
            if (msgs != null && msgs.size() > 0) {
                _latestMsgArrivedTime.set(System.currentTimeMillis());
                String topic = context.getMessageQueue().getTopic();
                for (MessageExt msg : msgs) {
                	String msgID = msg.getMsgId();
                	
                    byte[] body = msg.getBody();
                    if (body.length == 2 && body[0] == 0 && body[1] == 0) {
                        if (msg.getTopic().equals(RaceConfig.MqPayTopic)) {
                            _paymentMsgEndSignal.set(true);
                        }
                        LOG.info("Got the end signal");
                        continue;
                    }

                    if (RaceConfig.MqPayTopic.equals(topic)) {
//                        if (DEBUG_ENABLE) {
//                          int tmpCount = DEBUG_receivedMsgCount.addAndGet(1);
//                          FileUtil.appendLineToFile("/home/admin/receive.txt", DEBUG_thisSpoutName + ":DEBUG_receivedMsgCount " + tmpCount);
//                        }

                        PaymentMessage paymentMessage = RaceUtils
                                .readKryoObject(PaymentMessage.class, body);
                        if (paymentMessage.getPayAmount() > Constants.ZERO_THREHOLD) {
                            sendingQueue.offer(new MetaMessage(paymentMessage,
                                    topic, msgID));
                        } else {
//                            if (DEBUG_ENABLE) {
//                                int tmpCount2 = DEBUG_amountEqualsZeroPaymentMsgCount.addAndGet(1);
//                                FileUtil.appendLineToFile("/home/admin/zero.txt", DEBUG_thisSpoutName + ":DEBUG_amountEqualsZeroPaymentMsgCount " + tmpCount2);
//                            }
                        }
                    } else if (RaceConfig.MqTmallTradeTopic.equals(topic)
                            || RaceConfig.MqTaobaoTradeTopic.equals(topic)) {
//                        if (DEBUG_ENABLE) {
//                          int tmpCount = DEBUG_receivedMsgCount.addAndGet(1);
//                          FileUtil.appendLineToFile("/home/admin/receive.txt", DEBUG_thisSpoutName + ":DEBUG_receivedMsgCount " + tmpCount);
//                        }
                        OrderMessage orderMessage = RaceUtils.readKryoObject(
                                OrderMessage.class, body);
                        sendingQueue
                                .offer(new MetaMessage(orderMessage, topic, msgID));
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Failed in consumeMessage.", e);
            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
        }

        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }

    
    @Override
    public void ack(Object arg0) {
        // TODO Auto-generated method stub

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

    public void logDebugInfo() {
//        LOG.info("[AllSpout.logDebugInfo] DEBUG_payMessageCount:{}",
//                DEBUG_receivedPaymentMsgCount);
//        LOG.info(
//                "[AllSpout.logDebugInfo] DEBUG_amountEqualsZeroPaymentMsgCount:{}",
//                DEBUG_amountEqualsZeroPaymentMsgCount);
//        LOG.info("[AllSpout.logDebugInfo] DEBUG_sendTupleCount:{}",
//                DEBUG_sendTupleCount);
//        LOG.info("[AllSpout.logDebugInfo] DEBUG_resendCount:{}",
//                DEBUG_resendCount);
    }
}
