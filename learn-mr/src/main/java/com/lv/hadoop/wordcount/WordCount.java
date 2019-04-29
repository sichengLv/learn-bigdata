package com.lv.hadoop.wordcount;

import com.lv.hadoop.utils.HadoopUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * word count
 *
 * @author sichengLv
 * @date 2019-3-29
 */
public class WordCount extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new WordCount(), args);
    }

    /**
     * 提交MR作业
     *
     * @param strings 运行参数
     */
    @Override
    public int run(String[] strings) throws Exception {
        if (strings.length < 1) {
            System.out.println("Usage: hadoop jar xxx.jar <input> <output>");
            return 0;
        }

        String inputPath = strings[0];
        String outputPath = strings[1];

        Configuration conf = HadoopUtil.getConfig();
        Job job = Job.getInstance(conf);

        job.setJarByClass(WordCount.class);
        job.setJobName(this.getClass().getSimpleName());

        HadoopUtil.deleteFileIfExist(conf, outputPath);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        // 输入格式(seqFile)
        job.setInputFormatClass(TextInputFormat.class);
        // 默认输入格式
        // job.setInputFormatClass(TextInputFormat.class);
        // 默认输出格式
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        // 设置 reducer数量
        job.setNumReduceTasks(2);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

        return 0;
    }

    /**
     * Mapper类: 拆分单词
     */
    public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private String str;
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // StringTokenizer是一个用来分隔String的应用类，split函数
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreElements()) {
                str = itr.nextToken();
                word.set(str);
                System.out.println(str);

                context.write(word, one);
            }
        }
    }

    /**
     * Reducer类: 统计单词
     */
    public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

}
