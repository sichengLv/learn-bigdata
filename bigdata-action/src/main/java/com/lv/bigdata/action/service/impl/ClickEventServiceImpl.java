package com.lv.bigdata.action.service.impl;

import com.lv.bigdata.action.dao.ClickEventDAO;
import com.lv.bigdata.action.dao.po.ClickEventPO;
import com.lv.bigdata.action.service.ClickEventService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author lvsicheng
 * @date 2019-05-05 10:53
 */
@Service
public class ClickEventServiceImpl implements ClickEventService {

    @Autowired
    private ClickEventDAO clickEventDAO;

    /**
     * 获取最热 topic的 topN 列表
     *
     * @param topN topN
     * @return topN 列表
     */
    @Override
    public List<ClickEventPO> getTopN(Integer topN) {
        return clickEventDAO.selectTopN(topN);
    }

    /**
     * 获取 topic总数
     *
     * @return total topic
     */
    @Override
    public Long getTotalTopic() {
        return clickEventDAO.selectTopicCount();
    }

    /**
     * 获取 query总数
     *
     * @return total query
     */
    @Override
    public Long getTotalQuery() {
        return clickEventDAO.selectQueryCount();
    }

}
