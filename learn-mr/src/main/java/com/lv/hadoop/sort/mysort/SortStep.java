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

public class SortStep extends Configured implements Tool {

    private static final String INPUT_PATH = "hdfs://192.168.1.11:9000/user/lv/sort/output";
    private static final String OUTPUT_PATH = "hdfs://192.168.1.11:9000/user/lv/sort/output2";

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = HadoopUtil.getConfig();
        Job job = Job.getInstance(conf);
        job.setJarByClass(SumStep.class);
        job.setJobName(this.getClass().getSimpleName());

        HadoopUtil.deleteFileIfExist(conf, OUTPUT_PATH);

        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        job.setMapOutputKeyClass(InfoBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(InfoBean.class);
        job.setOutputValueClass(NullWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

    static class SortMapper extends Mapper<LongWritable, Text, InfoBean, NullWritable> {
        private InfoBean k2 = new InfoBean();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            String account = fields[0];
            double in = Double.parseDouble(fields[1]);
            double out = Double.parseDouble(fields[2]);
            k2.set(account, in, out);
            context.write(k2, NullWritable.get());
        }
    }

    static class SortReducer extends Reducer<InfoBean, NullWritable, InfoBean, NullWritable> {
        @Override
        protected void reduce(InfoBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    public static void main(String[] args) {
        try {
            ToolRunner.run(new SortStep(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
