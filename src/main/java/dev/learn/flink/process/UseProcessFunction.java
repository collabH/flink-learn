package dev.learn.flink.process;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @fileName: UseProcessFunction.java
 * @description: 使用processFunction
 * @author: by echo huang
 * @date: 2020/9/1 8:57 下午
 */
public class UseProcessFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> datasource = env.fromElements(1, 2, 3, 4, 5, 6);

        datasource.process(new MyProcessFunction())
                .print();
        env.execute();
    }
}

class MyProcessFunction extends ProcessFunction<Integer, String> {

    @Override
    public void open(Configuration parameters) throws Exception {
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        System.out.println("timestamp:" + timestamp);
        System.out.println("timeDomain:" + ctx.timeDomain());
        //  输出到downOperator
        out.collect("onTimer:" + 1);
    }

    @Override
    public void processElement(Integer value, Context context, Collector<String> collector) throws Exception {
        // 设置mark
        // 获取timeService
        TimerService timerService = context.timerService();

        // 获取当前waterMark
        long watermark = timerService.currentWatermark();

        // 输出到downOperator
        collector.collect("process:" + value);

    }
}
