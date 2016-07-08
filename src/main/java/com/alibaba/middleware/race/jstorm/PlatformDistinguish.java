package com.alibaba.middleware.race.jstorm;

import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.model.MetaMessage;
import com.alibaba.middleware.race.util.FileUtil;
import com.alibaba.middleware.unused.OldRaceTopology;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class PlatformDistinguish implements IRichBolt, Runnable {
    private static Logger LOG = LoggerFactory
            .getLogger(PlatformDistinguish.class);

    private static final long serialVersionUID = -8918483233950498761L;
    private OutputCollector _collector;
    private static final long SEND_INTERVAL = 30000L;
    private static final HashSet<String> receivedPayMsgTokenSet = new HashSet<String>(RaceConfig.MapInitCapacity);

//    private AtomicLong DEBUG_solveFailedCount = new AtomicLong(0);//TODO

    private LinkedBlockingQueue<MetaMessage> _unsolvedPayMessageQueue;

    private Map<Long, Double> TMTradeMessage;
    private Map<Long, Double> TBTradeMessage;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(OldRaceTopology.TBPAYSTREAM, new Fields("time",
                "amount"));
        declarer.declareStream(OldRaceTopology.TMPAYSTREAM, new Fields("time",
                "amount"));
        declarer.declareStream(RaceTopology.ALLPAYSTREAM, new Fields("time",
                "amount", "payPlatform"));
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
        MetaMessage metaTuple = (MetaMessage) tuple.getValue(1);
        if (RaceConfig.MqPayTopic.equals(metaTuple.getTopic())) {
            if (receivedPayMsgTokenSet.add(metaTuple.getMsgID())) {
                // emit pc or wireless amout
                Values values = new Values(
                        metaTuple.getCreateTime() / 60000 * 60,
                        metaTuple.getPayAmount(),
                        metaTuple.getPayPlatform());
                    _collector.emit(RaceTopology.ALLPAYSTREAM, values);
//                    FileUtil.appendLineToFile("/home/admin/ALLPAYSTREAM.txt", metaTuple.toString());
                if (solvePaymentMessageAndSend(metaTuple)) {

                } else {
//                    DEBUG_solveFailedCount.addAndGet(1);
                    _unsolvedPayMessageQueue.offer(metaTuple);
                }
            } else {
//                FileUtil.appendLineToFile("/home/admin/alreadyExist.txt", String.valueOf(metaTuple.getUniqueMsgToken()));
            }
        } else if (RaceConfig.MqTaobaoTradeTopic.equals(metaTuple.getTopic())) {
            if (TBTradeMessage.get(metaTuple.getOrderId()) == null) {
                TBTradeMessage.put(metaTuple.getOrderId(), metaTuple.getPayAmount());
            } else {
//                FileUtil.appendLineToFile("/home/admin/alreadyExist.txt", "order");
            }
        } else {
            if (TMTradeMessage.get(metaTuple.getOrderId()) == null) {
                TMTradeMessage.put(metaTuple.getOrderId(), metaTuple.getPayAmount());
            } else {
//                FileUtil.appendLineToFile("/home/admin/alreadyExist.txt", "order");
            }
        }
        // _collector.ack(tuple);//TODO
    }

    private boolean solvePaymentMessageAndSend(MetaMessage paymentMessage) {
        boolean ret = false;
        long orderId = paymentMessage.getOrderId();
        if (TBTradeMessage.containsKey(orderId)) {
            // send payment message
            Values values = new Values(
                    paymentMessage.getCreateTime() / 60000 * 60,
                    paymentMessage.getPayAmount());
            _collector.emit(OldRaceTopology.TBPAYSTREAM, values);
//            FileUtil.appendLineToFile("/home/admin/TBPAYSTREAM.txt", paymentMessage.toString());
//            LOG.info("PlatformDistinguish Emit TBPayment" + ":"
//                    + paymentMessage.toString());
            // update related order info
            Double lastAmount = TBTradeMessage.get(orderId);
            Double thisPayAmount = paymentMessage.getPayAmount();
            if (lastAmount - thisPayAmount < 1e-6) {
                TBTradeMessage.remove(orderId);
            } else {
                TBTradeMessage.put(orderId, lastAmount - thisPayAmount);
            }
            ret = true;
        } else if (TMTradeMessage.containsKey(orderId)) {
            // send payment message
            Values values = new Values(
                    paymentMessage.getCreateTime() / 60000 * 60,
                    paymentMessage.getPayAmount());
            _collector.emit(OldRaceTopology.TMPAYSTREAM, values);
//            FileUtil.appendLineToFile("/home/admin/TMPAYSTREAM.txt", paymentMessage.toString());
//            LOG.info("PlatformDistinguish Emit TMPayment" + ":"
//                    + paymentMessage.toString());
            // update related order info
            Double lastAmount = TMTradeMessage.get(orderId);
            Double thisPayAmount = paymentMessage.getPayAmount();
            if (lastAmount - thisPayAmount < 1e-6) {
                TMTradeMessage.remove(orderId);
            } else {
                TMTradeMessage.put(orderId, lastAmount - thisPayAmount);
            }
            ret = true;
        } else {
            ret = false;
        }
        return ret;
    }

    @Override
    public void prepare(Map conf, TopologyContext context,
            OutputCollector collector) {
        // TODO Auto-generated method stub
        this._collector = collector;

        _unsolvedPayMessageQueue = new LinkedBlockingQueue<MetaMessage>();

        TMTradeMessage = new ConcurrentHashMap<Long, Double>(
                RaceConfig.MapInitCapacity);

        TBTradeMessage = new ConcurrentHashMap<Long, Double>(
                RaceConfig.MapInitCapacity);

        new Thread(this, "solvePayMessageQueueAndSend").start();
    }

    @Override
    public void run() {
        while (true) {
            if (_unsolvedPayMessageQueue.isEmpty()) {
                try {
                    Thread.sleep(SEND_INTERVAL);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            for (int i = 0; i < _unsolvedPayMessageQueue.size(); ++i) {
                MetaMessage paymentMessage = _unsolvedPayMessageQueue.poll();
                if (paymentMessage != null) {
                    if (!solvePaymentMessageAndSend(paymentMessage)) {
                        _unsolvedPayMessageQueue.offer(paymentMessage);
//                        DEBUG_solveFailedCount.addAndGet(1);
//                        if (DEBUG_solveFailedCount.get() > 2000000) {// TODO
//                            LOG.info("DEBUG_solveFailed" + ":"
//                                    + paymentMessage.toString());
//                        }
                    }
                }
            }
        }
    }

}
