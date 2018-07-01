package com.hadoop.learn.weather;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Weather implements WritableComparable {

    private Integer year;
    private Integer month;
    private Integer day;
    private Integer temperature;

    public Integer getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public Integer getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public Integer getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public Integer getTemperature() {
        return temperature;
    }

    public void setTemperature(int temperature) {
        this.temperature = temperature;
    }

    @Override
    public int compareTo(Object o) {
        Weather w = (Weather) o;
        int res1 = Integer.compare(year, w.getYear());
        if (res1 == 0) {
            int res2 = Integer.compare(month, w.getMonth());
            if (res2 == 0) {
                return Integer.compare(w.getTemperature(), temperature);
            }
            return res2;
        }
        return res1;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(year);
        dataOutput.writeInt(month);
        dataOutput.writeInt(day);
        dataOutput.writeInt(temperature);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        year = dataInput.readInt();
        month = dataInput.readInt();
        day = dataInput.readInt();
        temperature = dataInput.readInt();
    }
}
