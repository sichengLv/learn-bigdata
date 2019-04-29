package com.lv.hadoop.sort.total;

import com.lv.hadoop.utils.HadoopUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TestApp extends Configured implements Tool {

    private static final String INPUT_PATH = "hdfs://node-01:9000/user/lv/sort/common_friends/temp.seq";
    private static final String OUTPUT_PATH = "hdfs://node-01:9000/user/lv/sort/common_friends/output";

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
        // 默认输入格式
//        job.setInputFormatClass(TextInputFormat.class);
        // 默认输出格式
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        // 设置 reducer数量
        job.setNumReduceTasks(3);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        // 设置分区类
        job.setPartitionerClass(TotalOrderPartitioner.class);
        // 设置存放采样后分区文件的路径
        TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), new Path("/user/lv/sort/common_friends/part.dat"));

        /**
         *第一个参数 freq: 表示来一个样本，将其作为采样点的概率。如果样本数目很大
         *第二个参数 numSamples：表示采样点最大数目为，我这里设置1000代表我的采样点最大为1000，如果超过1000，那么每次有新的采样点生成时
         * ，会删除原有的一个采样点,此参数大数据的时候尽量设置多一些
         * 第三个参数 maxSplitSampled：表示的是最大的分区数：我这里设置10不会起作用，因为我设置的分区只有4个而已
         */
        // 采样器 RandomSampler
        InputSampler.Sampler<IntWritable, Text> sampler = new InputSampler.RandomSampler<>(0.1, 1000, 10);
        // 将采样数据写入分区文件
        InputSampler.writePartitionFile(job, sampler);

        // 将分区文件放入分布式缓存
//        String partitionFile = TotalOrderPartitioner.getPartitionFile(conf);
//        URI partitionUri = new URI(partitionFile + "#" + TotalOrderPartitioner.DEFAULT_PATH);
//        job.addCacheFile(partitionUri);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

        return 0;
    }
}
