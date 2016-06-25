package com.alibaba.middleware.race.util;

import com.alibaba.middleware.race.Constants;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.rocketmq.CounterFactory;
import com.alibaba.middleware.race.util.FileUtil;

public class GetResultFromTairAndClear {

    public static void main(String [] args) throws Exception {
        FileUtil.deleteFileIfExist(Constants.ACTUAL_RESULT_FILE);
        TairOperatorImpl tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
                RaceConfig.TairGroup, RaceConfig.TairNamespace);
        Long[] timeStamp = CounterFactory.timeStamp;
        
        System.out.println("Starting...");
        for(int i = 0; i < timeStamp.length; i++){
        	String key = RaceConfig.prex_taobao + timeStamp[i];
        	Double value = (Double) tairOperator.get(key);
        	outputResult(key, value);
            tairOperator.remove(key);
        	
        	key = RaceConfig.prex_tmall + timeStamp[i];
        	value = (Double) tairOperator.get(key);
        	outputResult(key, value);
            tairOperator.remove(key);
        	
        	key = RaceConfig.prex_ratio + timeStamp[i];
        	value = (Double) tairOperator.get(key);
        	outputResult(key, value);
            tairOperator.remove(key);
        }
        tairOperator.close();
        System.out.println("End!");
        System.exit(0);
    }

    private static void outputResult(String key, Double value) {
//        System.out.println(key + " : " + value);
        if (value != null && value.compareTo(0.0) > 0) {
            FileUtil.appendLineToFile(Constants.ACTUAL_RESULT_FILE, key + " : " + value);
        }
    }
}
