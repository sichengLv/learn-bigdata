package com.lv.hadoop.sort.total;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class MyMapper extends Mapper<IntWritable, Text, IntWritable, Text> {
    @Override
    protected void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(key, value);
    }
}
