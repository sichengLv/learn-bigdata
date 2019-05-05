package com.lv.bigdata.action.dao.po;

/**
 * 用户点击事件
 *
 * @author lvsicheng
 * @date 2019-04-27 18:41
 */
public class ClickEventPO extends BasePO {

    private static final long serialVersionUID = 5719346202826828546L;

    /**
     * 新闻主题名称
     */
    private String topic;

    /**
     * 每个主题的计数
     */
    private Long count;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "ClickEventPO{" +
                "topic='" + topic + '\'' +
                ", count=" + count +
                '}';
    }
}
