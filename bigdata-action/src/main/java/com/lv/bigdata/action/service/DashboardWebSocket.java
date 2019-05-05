package com.lv.bigdata.action.service;

import com.alibaba.fastjson.JSON;
import com.lv.bigdata.action.dao.po.ClickEventPO;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.socket.server.standard.SpringConfigurator;

import javax.websocket.OnMessage;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * WebSocket与前台后台对接实现全双工通信
 *
 * @author lvsicheng
 * @date 2019-05-05 14:05
 */
@ServerEndpoint(value = "/websocket", configurator = SpringConfigurator.class)
public class DashboardWebSocket {

    private static final Logger LOGGER = Logger.getLogger(DashboardWebSocket.class);

    @Autowired
    private ClickEventService clickEventService;

    /**
     * 发送消息
     */
    @OnMessage
    public void onMessage(String msg, Session session) throws IOException, InterruptedException {
        String[] topicNames = new String[10];
        Long[] topicCounts = new Long[10];
        Long[] topicSum = new Long[1];

        while (true) {
            Map<String, Object> map = new HashMap<String, Object>();

            List<ClickEventPO> topN = clickEventService.getTopN(10);
            LOGGER.info("ClickEvent TOP10: " + Objects.toString(topN));

            for (int i = 0; i < topN.size(); i++) {
                topicNames[i] = topN.get(i).getTopic();
                topicCounts[i] = topN.get(i).getCount();
            }
            topicSum[0] = clickEventService.getTotalTopic();

            map.put("topicName", topicNames);
            map.put("topicCount", topicCounts);
            map.put("topicSum", topicSum);
            LOGGER.info("topic statistic: " + JSON.toJSONString(map));

            session.getBasicRemote().sendText(JSON.toJSONString(map));
            map.clear();

            Thread.sleep(1000);
        }
    }

}
