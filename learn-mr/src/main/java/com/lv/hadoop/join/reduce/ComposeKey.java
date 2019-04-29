package com.lv.hadoop.join.reduce;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 构造组合Key: <cid, flag>
 * 1. 排序规则：客户在前, 订单在后
 * 2. 类型相同：按 id升序
 */
public class ComposeKey implements WritableComparable<ComposeKey> {

    private String cid;
    private int flag;   // 0:客户 1:订单

    public String getCid() {
        return cid;
    }

    public void setCid(String cid) {
        this.cid = cid;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    /**
     * bean中定义 CopmareTo()是在 spill和 merge时用来来排序的(即用于分区内的排序)
     */
    @Override
    public int compareTo(ComposeKey obj) {
        int cmp = this.cid.compareTo(obj.cid);
        if (cmp == 0) {
            cmp = this.flag - obj.flag;
        }
        return cmp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.cid);
        out.writeInt(this.flag);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.cid = in.readUTF();
        this.flag = in.readInt();
    }

    @Override
    public String toString() {
        return "ComposeKey{" +
                "cid='" + cid + '\'' +
                ", flag=" + flag +
                '}';
    }
}
