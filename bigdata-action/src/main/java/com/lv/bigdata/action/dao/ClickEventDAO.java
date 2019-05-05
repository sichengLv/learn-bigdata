package com.lv.bigdata.action.dao;

import com.lv.bigdata.action.dao.po.ClickEventPO;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * 用户点击事件 DAO
 *
 * @author lvsicheng
 * @date 2019-04-27 18:45
 */
@Repository
public interface ClickEventDAO {

    /**
     * Top N
     *
     * @param topN top n
     * @return top n列表
     */
    List<ClickEventPO> selectTopN(Integer topN);

    /**
     * 统计一共有多少个topic
     *
     * @return topic个数
     */
    Long selectTopicCount();

    /**
     * 统计一共查询了多少次
     *
     * @return 查询次数
     */
    Long selectQueryCount();

}
