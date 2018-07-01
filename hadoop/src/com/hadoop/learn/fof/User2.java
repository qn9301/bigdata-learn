package com.hadoop.learn.fof;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class User2 implements WritableComparable<User2> {

    String me;
    String friend;
    Integer relation;

    public Integer getRelation() {
        return relation;
    }

    public void setRelation(Integer relation) {
        this.relation = relation;
    }



    public String getMe() {
        return me;
    }

    public void setMe(String me) {
        this.me = me;
    }

    public String getFriend() {
        return friend;
    }

    public void setFriend(String friend) {
        this.friend = friend;
    }

    public String format() {
        return me + "-" + friend;
    }

    @Override
    public int compareTo(User2 o) {
        return format().compareTo(o.format());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(me);
        dataOutput.writeUTF(friend);
        dataOutput.writeInt(relation);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        me = dataInput.readUTF();
        friend = dataInput.readUTF();
        relation = dataInput.readInt();
    }
}
