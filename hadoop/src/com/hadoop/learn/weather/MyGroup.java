package com.hadoop.learn.weather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyGroup extends WritableComparator {
    public MyGroup() {
        super(Weather.class,true);
    }
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Weather w1 = (Weather) a;
        Weather w2 = (Weather) b;
        int res1 = w1.getYear().compareTo(w2.getYear());
        if (res1 == 0)
        {
            return w1.getMonth().compareTo(w2.getMonth());
        }
        return res1;
    }
}
