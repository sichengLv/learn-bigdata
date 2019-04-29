package com.lv.hadoop.sort.secondary;

import com.lv.hadoop.utils.HadoopUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;

import java.util.Random;

/**
 * 构造数据: 年份--气温
 */
public class PrepareSeqFileApp {

    public static void main(String[] args) throws Exception {
        Random r = new Random();
        Configuration conf = HadoopUtil.getConfig();
        FileSystem fs = FileSystem.get(conf);

        Writer w = SequenceFile.createWriter(fs, conf, new Path("/user/lv/sort/secondary/temp.seq"), Text.class, IntWritable.class);
        for (int i = 0; i < 10000; i++) {
            w.append(new Text("" + (1900 + r.nextInt(11))), new IntWritable(r.nextInt(60) - 30 + r.nextInt(10)));
        }
        w.append(new Text("1901"), new IntWritable(36));
        w.append(new Text("1902"), new IntWritable(32));
        w.append(new Text("1903"), new IntWritable(40));
        w.append(new Text("1904"), new IntWritable(38));
        w.append(new Text("1905"), new IntWritable(37));
        w.append(new Text("1906"), new IntWritable(44));
        w.append(new Text("1907"), new IntWritable(25));
        w.append(new Text("1908"), new IntWritable(36));
        w.append(new Text("1909"), new IntWritable(36));
        w.append(new Text("1910"), new IntWritable(38));

        w.close();
    }
}
