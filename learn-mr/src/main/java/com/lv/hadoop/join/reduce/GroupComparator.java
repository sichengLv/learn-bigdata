package com.lv.hadoop.join.reduce;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 按照 cid对组合Key进行分组
 */
public class GroupComparator extends WritableComparator {

    public GroupComparator() {
        super(ComposeKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        ComposeKey k1 = (ComposeKey) a;
        ComposeKey k2 = (ComposeKey) b;

        return k1.getCid().compareTo(k2.getCid());
    }
}
