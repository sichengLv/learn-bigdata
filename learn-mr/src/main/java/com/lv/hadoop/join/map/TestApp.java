package com.lv.hadoop.join.map;

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

import java.net.URI;

/**
 * map端 join
 * 1. 将小文件放到分布式缓存中
 * 2. 这个小文件会被本地化到个个 map任务节点, 然后在 map函数中进行连接
 */
public class TestApp extends Configured implements Tool {

    private static final String INPUT_PATH = "hdfs://node-01:9000/user/lv/join/map/orders.txt";
    private static final String OUTPUT_PATH = "hdfs://node-01:9000/user/lv/join/map/output";

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

        // 默认输入输出格式
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(MyMapper.class);

        // 添加客户文件到分布式缓存中
        job.addCacheFile(new URI("hdfs://node-01:9000/user/lv/join/map/customers.txt"));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

        return 0;
    }
}
