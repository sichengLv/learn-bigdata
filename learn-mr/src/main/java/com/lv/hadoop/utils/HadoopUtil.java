package com.lv.hadoop.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * hadoop工具类
 *
 * @author sichengLv
 * @date 2019-3-29
 */
public class HadoopUtil {

    private static final String MAPREDUCE_JOB_JAR = "mapreduce.job.jar";
    private static final String MAPREDUCE_FRAMEWORK_NAME = "mapreduce.framework.name";
    private static final String MAPREDUCE_APP_CROSSPLATFORM = "mapreduce.app-submission.cross-platform";

    /**
     * 设置集群配置
     *
     * @return conf
     */
    public static Configuration getConfig() throws IOException {
        Configuration conf = new Configuration();
        // conf.set(MAPREDUCE_JOB_JAR, "G:/BaiduNetdiskDownload/learn-mr/learn-mapreduce2/target/learn-mapreduce-1.0.jar");
        conf.set(MAPREDUCE_FRAMEWORK_NAME, "yarn");
        conf.set(MAPREDUCE_APP_CROSSPLATFORM, "true");
        return conf;
    }

    public static void deleteFileIfExist(Configuration conf, String outputPath) {

    }
}
