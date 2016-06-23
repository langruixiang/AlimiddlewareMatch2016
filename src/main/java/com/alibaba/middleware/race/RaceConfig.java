package com.alibaba.middleware.race;

import java.io.Serializable;

public class RaceConfig implements Serializable {
	private static final long serialVersionUID = 4441842861070190453L;
	public static String prex_tmall = "platformTmall_";
    public static String prex_taobao = "platformTaobao_";
    public static String prex_ratio = "ratio_";


//    public static String MQNameServerAddr = "192.168.1.101:9876"; 
    
    public static String JstormTopologyName = "race";
    public static String MetaConsumerGroup = "group";
    public static String MqPayTopic = "MiddlewareRaceTestData_Pay";
    public static String MqTmallTradeTopic = "MiddlewareRaceTestData_TMOrder";
    public static String MqTaobaoTradeTopic = "MiddlewareRaceTestData_TBOrder";
    public static String TairConfigServer = "192.168.1.105:5198";
    public static String TairSalveConfigServer = "";
    public static String TairGroup = "group_1";
    public static Integer TairNamespace = 0;
    
    public static short PC = 0;
    public static short Wireless = 1;
    
    public static Long specialTBOrderID = -1L;
    public static Long specialTMOrderID = -2L;
    
    public static long BoltInterval = 2000L;
    public static int MapInitCapacity = 50000;    
    public static int tradeQueuesize = 10;
    
    public static int MQBatchSize = 1000;
}
