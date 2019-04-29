package com.lv.hadoop.mr.common_friends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

/**
 * A:B,C,D,E,F
 * B:A,C,D,E
 * C:A,B,E
 * D:A,B,E
 * E:A,B,C,D
 * F:A
 */

public class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Text owner = new Text();
        Set<String> set = new TreeSet<>();

        String[] split = value.toString().split(":");
        owner.set(split[0]);
        String[] friends = split[1].split(",");
        for (String friend : friends) {
            set.add(friend);
        }
        friends = set.toArray(friends);
        for (int i = 0; i < friends.length; i++) {
            for (int j = i + 1; j < friends.length; j++) {
                String k2 = friends[i] + friends[j];
                context.write(new Text(k2), owner);
            }
        }
    }
}
