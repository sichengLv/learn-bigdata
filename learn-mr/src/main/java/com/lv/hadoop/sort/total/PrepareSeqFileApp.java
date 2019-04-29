package com.lv.hadoop.sort.total;

import com.lv.hadoop.utils.HadoopUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;

import java.util.Random;

public class PrepareSeqFileApp {

    public static void main(String[] args) throws Exception {
        Random r = new Random();
        Configuration conf = HadoopUtil.getConfig();

        FileSystem fs = FileSystem.get(conf);
        Writer w = SequenceFile.createWriter(fs, conf, new Path("/user/lv/sort/common_friends/temp.seq"), IntWritable.class, Text.class);

        for (int i = 0; i < 1000; i++) {
            w.append(new IntWritable(r.nextInt(100)), new Text("" + (1000 + i)));
        }
        w.close();
    }
}
