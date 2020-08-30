package dev.learn.flink.function;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;

/**
 * @fileName: MyRichMapFunction.java
 * @description: MyRichMapFunction.java类说明
 * @author: by echo huang
 * @date: 2020-08-30 13:32
 */
public class MyRichMapFunction extends RichMapFunction<String, String> {


    /**
     * 设置运行环境
     *
     * @param t
     */
    @Override
    public void setRuntimeContext(RuntimeContext t) {
        super.setRuntimeContext(t);
    }

    /**
     * 只处理一次
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public String map(String s) throws Exception {
        return null;
    }

    /**
     * 优雅关闭
     * @throws Exception
     */
    @Override
    public void close() throws Exception {

    }
}
