package dev.learn.flink.operator;

import dev.learn.flink.FlinkEnvUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @fileName: SideOutputOperator.java
 * @description: 旁路输出
 * @author: huangshimin
 * @date: 2022/12/1 8:43 PM
 */
public class SideOutputOperator {
    public static void main(String[] args) throws Exception {
        OutputTag<String> outputTag = new OutputTag<>("tag", TypeInformation.of(String.class));
        StreamExecutionEnvironment env = FlinkEnvUtils.getStreamEnv();
        SingleOutputStreamOperator<String> ds =
                env.fromElements("hello", "world", "hsm", "wy").process(new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String s, ProcessFunction<String, String>.Context context,
                                               Collector<String> collector) throws Exception {
                        if (s.startsWith("h")) {
                            collector.collect(s);
                        }
                        context.output(outputTag, s);
                    }
                }).returns(TypeInformation.of(String.class));
        // 正常的流
        ds.print("normal:");
        ds.getSideOutput(outputTag).print("sideOut:");
        env.execute();
    }
}
