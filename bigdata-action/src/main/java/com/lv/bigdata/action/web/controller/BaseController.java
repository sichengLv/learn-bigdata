package com.lv.bigdata.action.web.controller;

import org.springframework.stereotype.Controller;

import java.util.HashMap;
import java.util.Map;

/**
 * 基础 Controller
 *
 * @author lvsicheng
 * @date 2019-05-05 11:02
 */
@Controller
public class BaseController {

    /**
     * 状态码
     */
    protected static String RET_CODE = "0";

    /**
     * 返回信息提示
     */
    protected static String RET_MSG = "";

    /**
     * 成功标识
     */
    protected static final String SUCCESS = "SUCCESS";

    /**
     * 返回结果集
     */
    protected Map<String, Object> dataMap = new HashMap<>();

    /**
     * 封装结果集
     */
    protected final Map<String, Object> returnDate(Map<String, Object> data) {
        Map<String, Object> result = new HashMap<String, Object>();
        Map<String, Object> dataMap = new HashMap<String, Object>();

        if (null == RET_CODE) {
            RET_CODE = "";
        }
        if (null == RET_MSG) {
            RET_MSG = "";
        }

        dataMap.put("retCode", RET_CODE);
        dataMap.put("retMsg", RET_MSG);
        dataMap.put("data", data);
        result.put("result", dataMap);

        return result;
    }

}
