package com.alibaba.middleware.race.model;

import com.alibaba.middleware.race.Constants;

public class PaymentMessageExt extends PaymentMessage {

    private static final long serialVersionUID = -3480458913319354369L;
    
    public static final short UNSOLVED_PLATFORM = -1;
    
    private short _salerPlatform = UNSOLVED_PLATFORM;

    public PaymentMessageExt(PaymentMessage paymentMessage) {
        super(paymentMessage.getOrderId(), paymentMessage.getPayAmount(),
                paymentMessage.getPaySource(), paymentMessage.getPayPlatform(),
                paymentMessage.getCreateTime());
    }
    
    public PaymentMessageExt(long orderID, double payAmount, short paySource, short payPlatform, long createTime){
        super(orderID, payAmount, paySource, payPlatform, createTime);
    }

    public short get_salerPlatform() {
        return _salerPlatform;
    }

    public void set_salerPlatform(short _salerPlatform) {
        this._salerPlatform = _salerPlatform;
    }
    
    public boolean isSalerPlatformSolved() {
        return _salerPlatform != UNSOLVED_PLATFORM;
    }
    
    public boolean isSalerPlatformTB() {
        return _salerPlatform == Constants.TAOBAO;
    }
}
