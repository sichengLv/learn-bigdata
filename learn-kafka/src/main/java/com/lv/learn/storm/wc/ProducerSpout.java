package com.lv.learn.storm.wc;

import org.apache.log4j.Logger;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * 生产消息, 模拟发送一些英文句子
 */
public class ProducerSpout extends BaseRichSpout {

    private static final Logger LOGGER = Logger.getLogger(ProducerSpout.class);

    private List<String> records;

    // 随机器
    private Random random;

    // 发射器
    private SpoutOutputCollector outputCollector;

    public ProducerSpout(List<String> records) {
        this.records = records;
    }

    /**
     * 初始化操作
     *
     * @param map
     * @param topologyContext
     * @param spoutOutputCollector
     */
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.outputCollector = spoutOutputCollector;

        this.random = new Random();
    }

    /**
     * 生产数据
     */
    @Override
    public void nextTuple() {
        String record = records.get(random.nextInt(records.size()));
        Values values = new Values(record);
        // 向下游发送数据, Values需要与 Fields对应
        this.outputCollector.emit(values);

        LOGGER.info("======> Record emitted success, record: " + record);
    }

    /**
     * 定义向下游输出数据的字段信息, 下游 Bolt会根据字段名进行获取
     *
     * @param outputFieldsDeclarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("record"));
    }
}
