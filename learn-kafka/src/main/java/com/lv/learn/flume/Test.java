package com.lv.learn.flume;

/**
 * @author lvsicheng
 * @date 2019-04-28 15:05
 */
public class Test {

    public static void main(String[] args) {
        String str = "00:00:00\t00717725924582846\t[闪字吧]\t1 2\twww.shanziba.com/";
        String pattern = "\\s+";

        String[] split = str.split(pattern);
        for (String s : split) {
            System.out.println(s);
        }
    }
}
