package com.lv.learn.storm.wc;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class WordCountBolt extends BaseRichBolt {

    private static final Logger LOGGER = Logger.getLogger(WordCountBolt.class);

    private OutputCollector outputCollector = null;

    private Map<String, Long> wordMap = null;

    @Override

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;
        this.wordMap = new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {
        // 获取上游数据
        String word = tuple.getString(0);

        // 统计单词数量
        Long count = wordMap.get(word);
        if (null == count) {
            count = 0L;
        }
        ++count;
        wordMap.put(word, count);
        LOGGER.info("word=" + word + ", count=" + count);

        // this.outputCollector.emit(new Values(word, count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // 该bolt没有下游
    }

    /**
     * 清理资源
     */
    @Override
    public void cleanup() {
        LOGGER.info("Word count results:");
        Set<Map.Entry<String, Long>> entries = wordMap.entrySet();
        for (Map.Entry<String, Long> entry : entries) {
            LOGGER.info("word= " + entry.getKey() + ", count=" + entry.getValue());
        }
        super.cleanup();
    }

}
