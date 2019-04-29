package com.lv.learn.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

public class HBaseManager {

    private static final Logger LOGGER = Logger.getLogger(HBaseManager.class);

    private static Configuration conf = null;

    private static Connection connection = null;

    private Table htable = null;

    private ResultScanner resultScanner = null;

    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", "mas130");
        conf.set("hbase.client.scanner.caching", "100");
        conf.set("hbase.rpc.timeout", "6000");
        conf.set("ipc.socket.timeout", "2000");
        conf.set("hbase.client.retries.number", "3");
        conf.set("hbase.client.pause", "100");
        conf.set("zookeeper.recovery.retry", "3");
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            LOGGER.error("HBase Connection init failed ", e);
        }
    }


    /**
     * 查找
     */
    public static Result get(String table, String rowKey, Filter filter) {
        if (StringUtils.isBlank(table) || StringUtils.isBlank(rowKey)) {
            return null;
        }

        Table htable = null;
        Result rs = null;
        try {
            if (null == connection) {
                LOGGER.warn("Get connection error");
                return null;
            }

            htable = connection.getTable(TableName.valueOf(table));
            Get get = new Get(Bytes.toBytes(rowKey));
            get.setFilter(filter);
            rs = htable.get(get);
        } catch (IOException e) {
            LOGGER.error("Get htable exception", e);
        } finally {
            try {
                htable.close();
            } catch (IOException e) {
                LOGGER.error("Close htable exception", e);
            }
        }
        return rs;
    }

    public static void batchPut(String table, List<Put> data) {
        if (null == connection) {
            LOGGER.warn("Get connection error");
            return;
        }

        BufferedMutator mutator = null;
        try {
            mutator = connection.getBufferedMutator(TableName.valueOf(table));
            if (!data.isEmpty()) {
                mutator.mutate(data);
                mutator.flush();
            }
        } catch (IOException e) {
            LOGGER.error("Batch Put data exception", e);
        } finally {
            try {
                if (null != mutator) {
                    mutator.close();
                }
            } catch (IOException e) {
                LOGGER.error("Close mutator exception", e);
            }
        }
    }

}
