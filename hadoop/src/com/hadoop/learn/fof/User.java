package com.hadoop.learn.fof;

import org.apache.hadoop.io.WritableComparable;
import org.apache.commons.lang.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class User implements WritableComparable<User>{

    String friend1;

    String friend2;

    Integer relation;

    public User(){

    }

    public User(String friend1, String friend2, Integer relation) {
        this.friend1 = friend1;
        this.friend2 = friend2;
        this.relation = relation;
    }

    public String getFriend1() {
        return friend1;
    }

    public void setFriend1(String friend1) {
        this.friend1 = friend1;
    }

    public String getFriend2() {
        return friend2;
    }

    public void setFriend2(String friend2) {
        this.friend2 = friend2;
    }

    public Integer getRelation() {
        return relation;
    }

    public void setRelation(Integer relation) {
        this.relation = relation;
    }

    // 保证键名一致
    public String format() {
        int res = friend1.compareTo(friend2);
        System.out.println("=========");
        System.out.println(res);
        System.out.println(res < 0);
        System.out.println(res == -1);
        if (res < 0) {
            return friend2 + '-' + friend1;
        }
        return friend1 + '-' + friend2;
    }

    @Override
    public int compareTo(User o) {
        return format().compareTo(o.format());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(friend1);
        dataOutput.writeUTF(friend2);
        dataOutput.writeInt(relation);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        friend1 = dataInput.readUTF();
        friend2 = dataInput.readUTF();
        relation = dataInput.readInt();
    }
}
