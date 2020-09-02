package dev.learn.flink.runparams;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @fileName: UseParseApplicationParams.java
 * @description: 使用解析 application params
 * @author: by echo huang
 * @date: 2020/9/2 3:46 下午
 */
public class UseParseApplicationParams {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String input = parameterTool.get("input");
    }
}
