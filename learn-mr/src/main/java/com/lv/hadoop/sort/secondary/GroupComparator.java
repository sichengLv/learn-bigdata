package com.lv.hadoop.sort.secondary;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 按照 year对组合Key进行分组
 */
public class GroupComparator extends WritableComparator {

    public GroupComparator() {
        super(ComposeKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        ComposeKey k1 = (ComposeKey) a;
        ComposeKey k2 = (ComposeKey) b;

        return k1.getYear().compareTo(k2.getYear());
    }
}
