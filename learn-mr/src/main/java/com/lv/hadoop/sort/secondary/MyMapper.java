package com.lv.hadoop.sort.secondary;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class MyMapper extends Mapper<Text, IntWritable, ComposeKey, NullWritable> {

    private ComposeKey k2 = new ComposeKey();

    @Override
    protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
        k2.setYear(key);
        k2.setTemp(value);
        context.write(k2, NullWritable.get());
    }
}
