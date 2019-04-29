package com.lv.hadoop.sort.secondary;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ComposeKey implements WritableComparable<ComposeKey> {

    private Text year;
    private IntWritable temp;

    public Text getYear() {
        return year;
    }

    public void setYear(Text year) {
        this.year = year;
    }

    public IntWritable getTemp() {
        return temp;
    }

    public void setTemp(IntWritable temp) {
        this.temp = temp;
    }

    /**
     * bean中定义 CopmareTo()是在 spill和 merge时用来来排序的(即用于分区内的排序)
     *
     * @param obj
     * @return
     */
    @Override
    public int compareTo(ComposeKey obj) {
        int cmp = this.year.compareTo(obj.getYear());
        if (cmp == 0) {
            cmp = -this.temp.compareTo(obj.getTemp());
        }
        return cmp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.year.toString());
        out.writeInt(temp.get());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.year = new Text(in.readUTF());
        this.temp = new IntWritable(in.readInt());
    }
}
