package com.lv.learn.util;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * 获取配置工具类
 */
public class CustomConfiguration {

    private static Map<String, String> properties = new HashMap<>();

    private static final Logger logger = Logger.getLogger(CustomConfiguration.class);

    static {
        loadProperties("/hbase.properties");
    }

    /**
     * 解析配置文件
     *
     * @param filename 文件名
     */
    private static void loadProperties(String filename) {
        InputStream stream = null;
        try {
            // 读取jar包中的配置文件
            stream = CustomConfiguration.class.getResourceAsStream(filename);
            if (stream != null) {
                Properties prop = new Properties();
                prop.load(stream);

                Iterator<String> it = prop.stringPropertyNames().iterator();
                while (it.hasNext()) {
                    String key = it.next();
                    CustomConfiguration.setString(key, prop.getProperty(key));
                }
            }
        } catch (Exception e) {
            logger.error("配置文件" + filename + "加载异常:", e);
        } finally {
            try {
                if (stream != null)
                    stream.close();
            } catch (IOException e) {
                logger.error("close stream exception", e);
            }
        }
    }

    public static String getString(String key, String defaultv) {
        String value = properties.get(key);
        if (value == null || "".equals(value)) {
            return defaultv;
        } else {
            return value;
        }
    }

    public static String getString(String key) {
        return properties.get(key);
    }

    public static int getInt(String key) {
        return Integer.parseInt(properties.get(key));
    }

    public static int getInt(String key, int defaultv) {
        try {
            return Integer.parseInt(properties.get(key));
        } catch (Exception e) {
            return defaultv;
        }
    }

    public static boolean getBoolean(String key) {
        return Boolean.parseBoolean(properties.get(key));
    }

    public static boolean getBoolean(String key, boolean defaultv) {
        try {
            return Boolean.parseBoolean(properties.get(key));
        } catch (Exception e) {
            return defaultv;
        }
    }

    public static double getDouble(String key) {
        return Double.parseDouble(properties.get(key));
    }

    public static double getDouble(String key, double defaultv) {
        try {
            return Double.parseDouble(properties.get(key));
        } catch (Exception e) {
            return defaultv;
        }
    }

    public static void setString(String key, String value) {
        properties.put(key, value);
    }

    public static void main(String[] args) {
        System.out.println(CustomConfiguration.getString("hbase.zookeeper.property.clientPort"));
    }

}
