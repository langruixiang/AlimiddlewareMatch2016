package com.alibaba.middleware.race.model;

import java.io.Serializable;
import java.util.Random;


/**
 * ���Ǻ�̨RocketMq�洢�Ľ�����Ϣģ��������PaymentMessage��ѡ��Ҳ�����Զ���
 * ������Ϣģ�ͣ�ֻҪģ���и����ֶε����ͺ�˳���PaymentMessageһ����������Kryo
 * �����г���Ϣ
 */

public class PaymentMessage implements Serializable{

    private static final long serialVersionUID = -4721410670774102273L;

    private long orderId; //����ID

    private double payAmount; //���

    /**
     * Money��Դ
     * 0,֧����
     * 1,��������ȯ
     * 2,����
     * 3,����
     */
    private short paySource; //��Դ

    /**
     * ֧��ƽ̨
     * 0��pC
     * 1������
     */
    private short payPlatform; //֧��ƽ̨

    /**
     * �����¼����ʱ��
     */
    private long createTime; //13λ�������뼶ʱ���������Ҫ���ʱ�䶼��ָ��ʱ��

    //KryoĬ����Ҫ�޲������캯��
    public PaymentMessage() {
    }

    private static Random rand = new Random();

    public static PaymentMessage[] createPayMentMsg(OrderMessage orderMessage) {
        PaymentMessage [] list = new PaymentMessage[2];
        for (short i = 0; i < 2; i++) {
            PaymentMessage msg = new PaymentMessage();
            msg.orderId = orderMessage.getOrderId();
            msg.paySource = i;
            msg.payPlatform = (short) (i % 2);
            msg.createTime = orderMessage.getCreateTime() + rand.nextInt(100);
            msg.payAmount = 0.0;
            list[i] = msg;
        }

        list[0].payAmount = rand.nextInt((int) (orderMessage.getTotalPrice() / 2));
        list[1].payAmount = orderMessage.getTotalPrice() - list[0].payAmount;

        return list;
    }

    @Override
    public String toString() {
        return "PaymentMessage{" +
                "orderId=" + orderId +
                ", payAmount=" + payAmount +
                ", paySource=" + paySource +
                ", payPlatform=" + payPlatform +
                ", createTime=" + createTime +
                '}';
    }

    public long getOrderId() {
        return orderId;
    }

    public void setOrderId(long orderId) {
        this.orderId = orderId;
    }

    public double getPayAmount() {
        return payAmount;
    }

    public void setPayAmount(double payAmount) {
        this.payAmount = payAmount;
    }

    public short getPaySource() {
        return paySource;
    }

    public void setPaySource(short paySource) {
        this.paySource = paySource;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public short getPayPlatform() {
        return payPlatform;
    }
}
