package com.lv.hadoop.mr.max_temp;

import com.lv.hadoop.utils.HadoopUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by Administrator on 2017-11-14.
 */
public class TestMR extends Configured implements Tool {
    private static final String MAPREDUCE_JOB_JAR = "mapreduce.job.jar";
    private static final String MAPREDUCE_FRAMEWORK_NAME = "mapreduce.framework.name";
    private static final String MAPREDUCE_APP_CROSSPLATFORM = "mapreduce.app-submission.cross-platform";

    private static final String INPUT_PATH = "hdfs://192.168.1.11:9000/user/lv/ncdc";
    private static final String OUTPUT_PATH = "hdfs://192.168.1.11:9000/user/lv/output";

//    private static final String INPUT_PATH = "file:///F:/tmp/common_friends/data-input/ncdc";
//    private static final String OUTPUT_PATH = "file:///F:/tmp/common_friends/out";

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new TestMR(), args);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        conf.set(MAPREDUCE_JOB_JAR, "out/artifacts/mapreduce_jar/mapreduce.jar");
        conf.set(MAPREDUCE_FRAMEWORK_NAME, "yarn");
        conf.set(MAPREDUCE_APP_CROSSPLATFORM, "true");

        // core-default.xml, core-site.xml, mapred-default.xml, mapred-site.xml, yarn-default.xml, yarn-site.xml
        Job job = Job.getInstance(conf);

        job.setJarByClass(TestMR.class);
        job.setJobName("Max Temperature");

        HadoopUtil.deleteFileIfExist(conf, OUTPUT_PATH);

        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

        return 0;
    }
}
