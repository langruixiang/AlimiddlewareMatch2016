package com.alibaba.middleware.race.test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class MixedTest {
    public static void main(String[] args) {
        testDoubleFormat();
    }

    private static void testDoubleFormat() {
        Double ds = new Double(0.877656465);
        System.out.println(String.format("%.2f", ds));
    }

    private static void testDoubleCompare() {
        double d1 = 0.4242353654654646546;
        double d2 = 0.4242353654654646546;
        System.out.println(Double.compare(d1 - d2, 0));
    }

    private static void testMapRemove() {
        Map<Long, String> map = new HashMap<Long, String>();
        List<String> numberList = new LinkedList<String>();
        for (long i = 0; i < 10000; ++i) {
            map.put(i, String.valueOf(i));
        }

        Iterator<Map.Entry<Long, String>> it = map.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, String> entry = it.next();
            numberList.add(entry.getValue());
            it.remove();
        }
        System.out.println(numberList.size());
    }
}
