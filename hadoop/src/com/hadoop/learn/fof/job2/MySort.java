package com.hadoop.learn.fof.job2;

import com.hadoop.learn.fof.User2;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MySort extends WritableComparator {
    public MySort() {
        super(User2.class);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        User2 u1 = (User2) a;
        User2 u2 = (User2) b;
        int res1 = u1.format().compareTo(u2.format());
        if (res1 == 0) {
            return u2.getRelation().compareTo(u1.getRelation());
        }
        return res1;
    }
}
