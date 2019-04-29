package com.lv.hadoop.sort.secondary;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class YearPartitioner extends Partitioner<ComposeKey, NullWritable> {

    @Override
    public int getPartition(ComposeKey composeKey, NullWritable nullWritable, int numPartitions) {
        String year = composeKey.getYear().toString();
        return (year.hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
