package com.lv.learn.storm.wc;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.Arrays;
import java.util.List;

public class WordCountTopo {

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        TopologyBuilder builder = new TopologyBuilder();

        // 模拟数据
        List<String> records = Arrays.asList("A Storm cluster is superficially similar to a Hadoop cluster",
                "All coordination between Nimbus and the Supervisors is done through a Zookeeper cluster",
                "The core abstraction in Storm is the stream");

        // 创建 Spout Bolt实例
        ProducerSpout producerSpout = new ProducerSpout(records);
        WordSplitterBolt wordSplitBolt = new WordSplitterBolt();
        WordCountBolt wordCountBolt = new WordCountBolt();

        // 配置 spout
        builder.setSpout("spout-producer", producerSpout, 2)
                .setNumTasks(3);
        // 配置 split bolt, 上游为 spout
        builder.setBolt("bolt-split", wordSplitBolt).shuffleGrouping("spout-producer");
        // 配置 count bolt, 上游为 split bolt
        builder.setBolt("bolt-count", wordCountBolt).fieldsGrouping("bolt-split", new Fields("word"));

        // 提交 topology
        Config conf = new Config();
        String jobName = WordCountTopo.class.getSimpleName();
        if (args.length > 0) {
            String nimbus = args[0];
            conf.put(Config.NIMBUS_SEEDS, nimbus);
            conf.setNumWorkers(2);

            // 将 topo提交到 storm集群
            StormSubmitter.submitTopologyWithProgressBar(jobName, conf, builder.createTopology());
        } else {

            // 创建本地集群,利用LocalCluster,storm在程序启动时在本地自动建立一个集群,方便本地开发和调试
            LocalCluster cluster = new LocalCluster();
            // 创建拓扑实例,并提交到本地集群上运行
            cluster.submitTopology(jobName, conf, builder.createTopology());
        }

    }
}
