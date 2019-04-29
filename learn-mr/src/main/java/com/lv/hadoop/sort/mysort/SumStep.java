package com.lv.hadoop.sort.mysort;

import com.lv.hadoop.utils.HadoopUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * 编写 MR进行统计求和
 */
public class SumStep extends Configured implements Tool {

    private static final String INPUT_PATH = "hdfs://192.168.1.11:9000/user/lv/sort";
    private static final String OUTPUT_PATH = "hdfs://192.168.1.11:9000/user/lv/sort/output";

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = HadoopUtil.getConfig();
        Job job = Job.getInstance(conf);

        job.setJarByClass(SumStep.class);
        job.setJobName("SumStep");

        HadoopUtil.deleteFileIfExist(conf, OUTPUT_PATH);

        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

        job.setMapperClass(SumMapper.class);
        job.setReducerClass(SumReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(InfoBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(InfoBean.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

        return 0;
    }

    static class SumMapper extends Mapper<LongWritable, Text, Text, InfoBean> {
        private Text k = new Text();
        private InfoBean v = new InfoBean();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t");
            String account = fields[0];
            double in = Double.parseDouble(fields[1]);
            double out = Double.parseDouble(fields[2]);
            k.set(account);
            v.set(account, in, out);
            context.write(k, v);
        }
    }

    static class SumReducer extends Reducer<Text, InfoBean, NullWritable, InfoBean> {
        private InfoBean v = new InfoBean();

        @Override
        protected void reduce(Text key, Iterable<InfoBean> values, Context context) throws IOException, InterruptedException {
            double in_sum = 0;
            double out_sum = 0;
            for (InfoBean value : values) {
                in_sum += value.getIncome();
                out_sum += value.getExpense();
            }
            v.set(key.toString(), in_sum, out_sum);
            context.write(NullWritable.get(), v);
        }
    }

    public static void main(String[] args) {
        try {
            ToolRunner.run(new SumStep(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
