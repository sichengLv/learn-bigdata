package com.lv.hadoop.sort.secondary;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Define the comparator that controls which keys are grouped together for a single call to Reducer.reduce()
 */
public class MyReducer extends Reducer<ComposeKey, NullWritable, Text, IntWritable> {

    // 到达reduce时, year相同的组合Key都聚集在一起
    // 此时组合Key按照 year升序, temp降序  --> 所以第一个Key就是这一年的最高气温
    // 如果想要输出同一年份的所有气温, 需要进行迭代
    @Override
    protected void reduce(ComposeKey key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
//        for (NullWritable value : values) {
//            context.write(key.getYear(), key.getTemp());
//        }
        context.write(key.getYear(), key.getTemp());
    }
}
