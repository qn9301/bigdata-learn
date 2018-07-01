package com.hadoop.learn.fof.job1;

import com.hadoop.learn.fof.User;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MySort extends WritableComparator {

    public MySort() {
        super(User.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        User user1 = (User) a;
        User user2 = (User) b;
        return user1.format().compareTo(user2.format());
    }
}
