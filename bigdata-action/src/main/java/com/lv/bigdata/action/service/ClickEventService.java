package com.lv.bigdata.action.service;

import com.lv.bigdata.action.dao.po.ClickEventPO;

import java.util.List;

/**
 * 统计新闻热点情况
 *
 * @author lvsicheng
 * @date 2019-05-05 10:49
 */
public interface ClickEventService {

    /**
     * 获取最热 topic的 topN 列表
     *
     * @param topN top n
     * @return top n列表
     */
    List<ClickEventPO> getTopN(Integer topN);

    /**
     * 获取 topic总数
     *
     * @return total topic
     */
    Long getTotalTopic();

    /**
     * 获取 query总数
     *
     * @return total query
     */
    Long getTotalQuery();

}
