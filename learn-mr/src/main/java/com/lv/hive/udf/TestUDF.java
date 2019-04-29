package com.lv.hive.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * 自定义函数
 * 1.创建一个类, 继承 UDF类, 实现 evaluate()方法
 * 2.将这个类打成 jar包并上传到服务器
 * 3.将jar包添加到 hive类路径下       --> add jar testudf.jar
 * 4.创建临时/永久函数, 给类起一个别名 --> create temporary function testudf as 'com.lv.hive.udf.TestUDF'
 * 5.使用自定义函数     --> select testudf(1, 9)
 * 6.销毁不需要的函数   --> drop temporary function testudf;
 */
public class TestUDF extends UDF {

    public Text evaluate(Text str) {
        if (str == null) {
            return null;
        }
        Text res = new Text(StringUtils.strip(str.toString()));
        return res;
    }

    public int evaluate(int a, int b) {
        return a + b;
    }

}
