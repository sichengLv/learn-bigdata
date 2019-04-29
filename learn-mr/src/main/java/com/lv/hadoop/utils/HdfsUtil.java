package com.lv.hadoop.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * HDFS文件系统操作工具类
 */
public class HdfsUtil {

    /**
     * 删除指定的hdfs目录
     *
     * @param conf       集群配置
     * @param outputPath 目录路径
     */
    public static void deleteFileIfExist(Configuration conf, String outputPath) {
        try {
            FileSystem fs = FileSystem.get(URI.create(outputPath), conf);
            Path delPath = new Path(outputPath);
            if (fs.exists(delPath)) {
                fs.delete(delPath, true);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 上传本地文件至 HDFS
     */
    public static void uploadFile(FileSystem fs, String src, String dst) throws IOException {
        Path srcPath = new Path(src);
        Path dstPath = new Path(dst);

        fs.copyFromLocalFile(false, srcPath, dstPath);

        FileStatus[] fileStatuses = fs.listStatus(dstPath);
        for (FileStatus fileStatus : fileStatuses) {
            System.out.println(fileStatus.getPath());
        }
    }

}
