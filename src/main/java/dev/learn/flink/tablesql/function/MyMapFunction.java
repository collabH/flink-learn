package dev.learn.flink.tablesql.function;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * @fileName: MyMapFunction.java
 * @description: 自定义标量函数
 * @author: by echo huang
 * @date: 2020/9/4 11:57 下午
 */
public class MyMapFunction extends ScalarFunction {
    public @DataTypeHint("STRING") String eval(Integer a) {
        return "hello:" + a;
    }
}
