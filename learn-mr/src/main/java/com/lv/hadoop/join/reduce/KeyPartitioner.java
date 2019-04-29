package com.lv.hadoop.join.reduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class KeyPartitioner extends Partitioner<ComposeKey, Text> {

    @Override
    public int getPartition(ComposeKey composeKey, Text text, int numPartitions) {
        return (composeKey.getCid().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
