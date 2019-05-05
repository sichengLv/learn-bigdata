package com.lv.bigdata.action.web.controller;

import com.lv.bigdata.action.dao.po.ClickEventPO;
import com.lv.bigdata.action.service.ClickEventService;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author lvsicheng
 * @date 2019-05-05 11:11
 */
@Controller
@RequestMapping(value = "/dashboard")
public class DashBoardController extends BaseController {

    private static final Logger LOGGER = Logger.getLogger(DashBoardController.class);

    @Autowired
    private ClickEventService clickEventService;

    /**
     * 获取 topic总数和 query总数
     *
     * @return result
     */
    @RequestMapping("/total")
    @ResponseBody
    public Map<String, Object> totalCount() {
        Map<String, Object> map = new HashMap();

        Long totalTopic = clickEventService.getTotalTopic();
        Long totalQuery = clickEventService.getTotalQuery();

        map.put("totalTopic", totalTopic);
        map.put("totalQuery", totalQuery);

        return returnDate(map);
    }

    /**
     * 获取最热 topic的 topN 列表
     *
     * @return result
     */
    @RequestMapping("top")
    @ResponseBody
    public Map<String, Object> getTopN() {
        int topN = 10;
        final List<ClickEventPO> res = clickEventService.getTopN(topN);

        Map<String, Object> map = new HashMap() {{
            put("topN", res);
        }};

        return returnDate(map);
    }

}
