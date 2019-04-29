package com.lv.hadoop.join.reduce;

import com.lv.hadoop.utils.HadoopUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * reduce端 join
 * 1. 由于进行 join操作的文件都太大, 无法进行 map端 join
 * 2. 一个 map任务中, 既有来自文件a的数据, 也有文件b的数据, 那么该怎么区分?
 * 3. 确保相同连接字段的数据都进入同一分区
 * 4. 确保同一分区中: 小表数据在前, 大表数据在后
 * 5. 同一分区中: 进行分组
 */
public class TestApp extends Configured implements Tool {

    private static final String INPUT_PATH = "hdfs://node-01:9000/user/lv/join/reduce";
    private static final String OUTPUT_PATH = "hdfs://node-01:9000/user/lv/join/reduce/output";

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

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(ComposeKey.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(2);

        // 设置分区类
        job.setPartitionerClass(KeyPartitioner.class);
        // 设置分区内排序规则(默认会调用 compareTo方法, 也可以不用设置)
        job.setSortComparatorClass(KeyComparator.class);
        // 设置分组比较器, 按照key进行聚集
        job.setGroupingComparatorClass(GroupComparator.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

        return 0;
    }
}
