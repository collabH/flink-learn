package dev.learn.flink.tablesql.table.udf;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * @fileName: MyMapFunction.java
 * @description: MyMapFunction.java类说明
 * @author: by echo huang
 * @date: 2020/9/4 11:57 下午
 */
public class MyMapFunction extends ScalarFunction {
    public String eval(Integer a) {
        return "hello:" + a;
    }
}
