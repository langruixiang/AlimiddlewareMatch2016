package com.alibaba.middleware.race.util;

import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.Constants;
import com.alibaba.middleware.race.util.FileUtil;

public class CorrectRateCalculator {
    private static final double DIFF_THREHOLD = 1e-6;

    private static final Logger LOG = LoggerFactory.getLogger(CorrectRateCalculator.class);

    public static void main(String [] args) throws Exception {
        if (!FileUtil.isFileExist(Constants.EXPECTED_RESULT_FILE)) {
            LOG.error(Constants.EXPECTED_RESULT_FILE + " doesn't exist!");
            return;
        }
        if (!FileUtil.isFileExist(Constants.ACTUAL_RESULT_FILE)) {
            LOG.error(Constants.ACTUAL_RESULT_FILE + " doesn't exist!");
            return;
        }

        Map<String, Double> expectedResultMap = FileUtil.readHashMapFromFile(Constants.EXPECTED_RESULT_FILE, 5000);
        Map<String, Double> actualResultMap = FileUtil.readHashMapFromFile(Constants.ACTUAL_RESULT_FILE, 5000);
        
        double correctRate = calculateCorrectRate(expectedResultMap, actualResultMap);
        System.out.println("---------------------------------");
        System.out.println("correctRate :" + correctRate);
        System.out.println("---------------------------------");
    }

    /**
     * @param expectedResultMap
     * @param actualResultMap
     */
    private static double calculateCorrectRate(
            Map<String, Double> expectedResultMap,
            Map<String, Double> actualResultMap) {
        if (expectedResultMap.size() != actualResultMap.size()) {
            LOG.warn("expectedResultMap.size() != actualResultMap.size()");
        }
        int correctCnt = 0;
        for (Entry<String, Double> entry : expectedResultMap.entrySet()) {
            if(Math.abs(entry.getValue() - actualResultMap.get(entry.getKey())) < DIFF_THREHOLD) {
                ++correctCnt;
            }
        }
        System.out.println("correctCnt : " + correctCnt);
        return correctCnt * 1.0 / expectedResultMap.size();
    }
}
