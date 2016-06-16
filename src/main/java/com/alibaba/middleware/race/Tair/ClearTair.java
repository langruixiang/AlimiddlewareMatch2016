package com.alibaba.middleware.race.Tair;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.rocketmq.CounterFactory;

public class ClearTair {
	public static void main(String [] args) throws Exception {
        TairOperatorImpl tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
                RaceConfig.TairGroup, RaceConfig.TairNamespace);
        Long[] timeStamp = CounterFactory.timeStamp;
        
        for(int i = 0; i < timeStamp.length; i++){
        	String key = RaceConfig.prex_taobao + timeStamp[i];
        	tairOperator.remove(key);
        	
        	key = RaceConfig.prex_tmall + timeStamp[i];
        	tairOperator.remove(key);
        	
        	key = RaceConfig.prex_ratio + timeStamp[i];
        	tairOperator.remove(key);
        }
        
        System.out.println("Closing tair");
        tairOperator.close();
    }

}
