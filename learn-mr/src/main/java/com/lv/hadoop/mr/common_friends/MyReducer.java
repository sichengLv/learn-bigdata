package com.lv.hadoop.mr.common_friends;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String commonfriend = "";
        for (Text value : values) {
            if ("".equals(commonfriend)) {
                commonfriend = value.toString();
            } else {
                commonfriend = commonfriend + ":" + value.toString();
            }
        }
        context.write(key, new Text(commonfriend));
    }
}
