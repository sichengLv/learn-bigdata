
package com.lv.bigdata.action.service.weblog;

import java.io.*;

/**
 * 模拟产生用户点击日志
 * <p>
 * 不断读取"搜狗实验室"的用户查询日志作为输入, 然后将日志一条条写入到某个文件夹下
 *
 * @author lvsicheng
 * @date 2019-04-27 19:28
 */
public class GenerateLog {

    /**
     * 输入路径
     */
    private static String input;

    /**
     * 输出路径
     */
    private static String output;

    /**
     * 读取输入日志文件, 输出到output
     *
     * @param input  输入文件
     * @param output 输出
     */
    public void generator(String input, String output) {
        FileInputStream fis = null;
        InputStreamReader isr = null;
        BufferedReader br = null;

        FileOutputStream fos = null;
        OutputStreamWriter osw = null;
        BufferedWriter bw = null;

        try {
            fis = new FileInputStream(input);
            isr = new InputStreamReader(fis);
            br = new BufferedReader(isr);

            // 这里创建文件输出流的时候, 要指定 append为 true, 否则文件将被覆盖
            fos = new FileOutputStream(output, true);
            osw = new OutputStreamWriter(fos);
            bw = new BufferedWriter(osw);

            int count = 0;
            String str;
            while ((str = br.readLine()) != null) {
                count++;

                // 模拟源源不断地产生日志
                Thread.sleep(50);

                // 处理中文乱码, 直接修改源文件编码为UTF-8(另存为)
//                String line = new String(str.getBytes("UTF-8"));
                String line = str;
                System.out.println("row:" + count + " >>>>> " + line);

                // 生成的日志输出到output
                bw.write(line);
                bw.newLine();
                bw.flush();
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (null != fis) fis.close();
                if (null != isr) isr.close();
                if (null != br) br.close();

                if (null != fos) fos.close();
                if (null != osw) fos.close();
                if (null != bw) fos.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: <input> <output>");
        }

        input = args[0];
        output = args[1];

        GenerateLog generateLog = new GenerateLog();
        generateLog.generator(input, output);
    }


}
