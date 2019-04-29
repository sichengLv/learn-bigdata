package com.lv.hadoop.sort.secondary;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class KeyComparator extends WritableComparator {

    public KeyComparator() {
        super(ComposeKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        ComposeKey k1 = (ComposeKey) a;
        ComposeKey k2 = (ComposeKey) b;
        return k1.compareTo(k2);
    }
}
