package com.lv.bigdata.action.dao.po;

import java.io.Serializable;

/**
 * 基础PO
 *
 * @author lvsicheng
 * @date 2019-04-27 18:41
 */
public class BasePO implements Serializable {

    /**
     * 自增id
     */
    private Integer id;

    /**
     * 创建时间
     */
    private Long createTime;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Long createTime) {
        this.createTime = createTime;
    }

}
