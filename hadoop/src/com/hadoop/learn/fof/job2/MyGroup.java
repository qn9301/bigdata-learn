package com.hadoop.learn.fof.job2;

import com.hadoop.learn.fof.User2;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyGroup extends WritableComparator{
    public MyGroup() {
        super(User2.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        User2 user1 = (User2) a;
        User2 user2 = (User2) b;
        return user1.getMe().compareTo(user2.getMe());
    }
}
