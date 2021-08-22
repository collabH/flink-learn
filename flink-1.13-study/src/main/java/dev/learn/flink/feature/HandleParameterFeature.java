package dev.learn.flink.feature;

import dev.learn.flink.FlinkEnvUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

/**
 * @fileName: HandleParameterFeature.java
 * @description: 处理配置
 * @author: huangshimin
 * @date: 2021/8/22 10:30 下午
 */
public class HandleParameterFeature {
    public static void main(String[] args) throws Exception {
        // 加载properties
        String filePath = "test.properties";
        ParameterTool config1 = ParameterTool.fromPropertiesFile(filePath);

        //加载args
        ParameterTool config2 = ParameterTool.fromArgs(args);

        // 配置传递到全局 可以在rich算子的open方法中获取
        StreamExecutionEnvironment streamEnv = FlinkEnvUtils.getStreamEnv();
        streamEnv.getConfig().setGlobalJobParameters(config1);

        streamEnv.fromElements(1, 2, 3).map(new RichMapFunction<Integer, Object>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                ParameterTool globalJobParameters =
                        (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
                System.out.println(globalJobParameters.get("test"));
            }

            @Override
            public Object map(Integer value) throws Exception {
                return null;
            }
        });
        streamEnv.execute();
    }
}
