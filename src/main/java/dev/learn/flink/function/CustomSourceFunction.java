package dev.learn.flink.function;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @fileName: CustomSourceFunction.java
 * @description: CustomSourceFunction.java类说明
 * @author: by echo huang
 * @date: 2020-08-29 18:11
 */
public class CustomSourceFunction implements SourceFunction<String> {
    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        for (int i = 0; i < 10; i++) {
            sourceContext.collect("hello" + i);
        }
    }

    @Override
    public void cancel() {

    }

}
