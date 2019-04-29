package com.lv.hadoop.mr.max_temp;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Administrator on 2017-11-14.
 */
public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final int MISSING = 9999;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String year = line.substring(15, 19);
        int airTemp;
        if (line.charAt(87) == '+') {
            airTemp = Integer.parseInt(line.substring(88, 92));
        } else {
            airTemp = Integer.parseInt(line.substring(87, 92));
        }
        String quality = line.substring(92, 93);
        if (airTemp != MISSING && quality.matches("[01459]")) {
            context.write(new Text(year), new IntWritable(airTemp));
        }
        context.getCounter("group-1", "count-1").increment(1);
    }
}
