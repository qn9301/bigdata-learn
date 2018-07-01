package com.hadoop.learn.weather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MySort extends WritableComparator {
    public MySort() {
        super(Weather.class,true);
    }
    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        Weather w1 = (Weather) a;
        Weather w2 = (Weather) b;
        int res1 = w1.getYear().compareTo(w2.getYear());
        if (res1 == 0)
        {
            int res2 = w1.getMonth().compareTo(w2.getMonth());
            if (res2 == 0) {
                return w2.getTemperature().compareTo(w1.getTemperature());
            }
            return res2;
        }
        return res1;
    }
}
