package com.lv.learn.storm.wc;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * 将句子拆分成一个个单词并发射
 */
public class WordSplitterBolt extends BaseRichBolt {

    private static final Logger LOGGER = Logger.getLogger(WordSplitterBolt.class);

    private OutputCollector outputCollector;

    /**
     * 初始化
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;
    }

    /**
     * 处理 Tuple的方法
     *
     * @param tuple
     */
    @Override
    public void execute(Tuple tuple) {
        // 根据上游设置的字段名获取数据
        String record = tuple.getStringByField("record");
        if (StringUtils.isBlank(record)) {
            return;
        }

        // 分词
        String[] split = record.split("\\s+");
        for (String word : split) {
            // 把每个单词发送出去
            this.outputCollector.emit(new Values(word));
            this.outputCollector.ack(tuple);
        }

    }

    /**
     * 定义向下游输出数据的字段信息
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

}
