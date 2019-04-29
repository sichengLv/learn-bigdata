package com.lv.hadoop.join.reduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;


/**
 * 如何区分数据是来自客户, 还是订单?
 */
public class MyMapper extends Mapper<LongWritable, Text, ComposeKey, Text> {

    private String fileName;
    private ComposeKey ck = new ComposeKey();

    // 根据 FileSplit得到输入文件的路径名
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) context.getInputSplit();
        fileName = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(" ");

        // 判断是客户还是订单
        if (fileName.contains("customer")) {
            ck.setCid(split[0]);
            ck.setFlag(0);
        } else {
            ck.setCid(split[3]);
            ck.setFlag(1);
        }
        context.write(ck, value);
    }
}
