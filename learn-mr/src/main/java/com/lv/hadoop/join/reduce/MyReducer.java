package com.lv.hadoop.join.reduce;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * reduce端连接:
 * 由于进行了分组, cid相同归为同一组, 然后进行迭代输出, 第一个<k, {v}>就是客户, 剩下的都是该客户的订单
 */
public class MyReducer extends Reducer<ComposeKey, Text, Text, NullWritable> {
    @Override
    protected void reduce(ComposeKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iter = values.iterator();
        // 注意：要new一个Text, 而不能直接引用iter.next()
        Text customer = new Text(iter.next());
        context.getCounter("customer", customer.toString());
        while (iter.hasNext()) {
            Text order = iter.next();
            context.write(new Text(key.toString() + "->" + customer.toString() + "->" + order.toString()), NullWritable.get());
        }
    }
}
