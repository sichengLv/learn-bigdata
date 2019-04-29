package com.lv.hadoop.join.map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;


public class MyMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Map<String, String> map = new HashMap<>();

    // 从分布式缓存中读取客户信息并保存
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] files = context.getCacheFiles();
        context.getCounter("cacheFiles", files[0].toString());
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream fis = fs.open(new Path(files[0]));
        BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
        String line;
        while ((line = reader.readLine()) != null) {
            String[] arr = line.split(" ");
            map.put(arr[0], arr[0] + " " + arr[1]);
        }
    }

    // 进行连接
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] order = value.toString().split(" ");
        String customer = map.get(order[3]);
        context.write(new Text(customer + "->" + order[1] + " " + order[2] + " " + order[0]), NullWritable.get());
    }
}
