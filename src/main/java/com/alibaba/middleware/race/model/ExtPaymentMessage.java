package com.alibaba.middleware.race.model;

public class ExtPaymentMessage extends PaymentMessage {

    private static final long serialVersionUID = -3480458913319354369L;

    private boolean _isFromTB = true;

    public ExtPaymentMessage() {
    }
    
    public ExtPaymentMessage(long orderID, double payAmount, short paySource, short payPlatform, long createTime){
        super(orderID, payAmount, paySource, payPlatform, createTime);
    }

    public boolean get_isFromTB() {
        return _isFromTB;
    }

    public void set_isFromTB(boolean _isFromTB) {
        this._isFromTB = _isFromTB;
    }
}
