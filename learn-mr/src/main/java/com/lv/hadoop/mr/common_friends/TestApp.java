package com.lv.hadoop.mr.common_friends;

import com.lv.hadoop.utils.HadoopUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TestApp extends Configured implements Tool {

    private static final String INPUT_PATH = "hdfs://node-01:9000/user/lv/mr/friends.txt";
    private static final String OUTPUT_PATH = "hdfs://node-01:9000/user/lv/mr/output";

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new TestApp(), args);
    }

    @Override
    public int run(String[] strings) throws Exception {

        Configuration conf = HadoopUtil.getConfig();
        Job job = Job.getInstance(conf);

        job.setJarByClass(TestApp.class);
        job.setJobName(this.getClass().getSimpleName());

        HadoopUtil.deleteFileIfExist(conf, OUTPUT_PATH);

        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

        // 默认输入格式
        job.setInputFormatClass(TextInputFormat.class);
        // 默认输出格式
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        // 设置 reducer数量
//        job.setNumReduceTasks(3);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

        return 0;
    }
}
