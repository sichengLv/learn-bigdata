package com.lv.bigdata.action.service.impl;

import com.lv.bigdata.action.dao.ClickEventDAO;
import com.lv.bigdata.action.dao.po.ClickEventPO;
import com.lv.bigdata.action.service.BaseSpring4Test;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Objects;

/**
 * ClickEventService 单元测试
 */
public class ClickEventServiceImplTest extends BaseSpring4Test {

    @Autowired
    private ClickEventDAO clickEventDAO;

    @Test
    public void testGetTopN() {
        List<ClickEventPO> clickEventPOS = clickEventDAO.selectTopN(10);
        System.out.println(Objects.toString(clickEventPOS));
        Assert.assertTrue(clickEventPOS.size() > 0);
    }

    @Test
    public void testGetTotalTopic() {
        Long topicCount = clickEventDAO.selectTopicCount();
        Assert.assertNotNull(topicCount);
    }

    @Test
    public void testGetTotalQuery() {
        Long queryCount = clickEventDAO.selectQueryCount();
        Assert.assertNotNull(queryCount);
    }

}
