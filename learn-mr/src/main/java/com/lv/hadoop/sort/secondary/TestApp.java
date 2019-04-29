package com.lv.hadoop.sort.secondary;

import com.lv.hadoop.utils.HadoopUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 二次排序(即对 value进行排序)
 * <p>
 * 1. 构造组合 key
 * 2. map端：按照 key进行分区
 * 3. reduce端：按照 key进行分组
 * 4. 分区内按照组合key进行排序
 */
public class TestApp extends Configured implements Tool {

    private static final String INPUT_PATH = "hdfs://node-01:9000/user/lv/sort/secondary/temp.seq";
    private static final String OUTPUT_PATH = "hdfs://node-01:9000/user/lv/sort/secondary/output";

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

        // 输入格式(seqFile)
        job.setInputFormatClass(SequenceFileInputFormat.class);
        // 默认输出格式
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(ComposeKey.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(2);

        // 设置分区类
        job.setPartitionerClass(YearPartitioner.class);
        // 设置分区内排序规则(默认会调用 compareTo方法, 也可以不用设置)
        job.setSortComparatorClass(KeyComparator.class);
        // 设置分组比较器, 按照key进行聚集
        job.setGroupingComparatorClass(GroupComparator.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

        return 0;
    }
}
