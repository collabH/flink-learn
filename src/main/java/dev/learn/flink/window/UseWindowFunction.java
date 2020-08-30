package dev.learn.flink.window;

import dev.learn.flink.function.SplitFunction;
import dev.learn.flink.function.SumReduceFunction;
import dev.learn.flink.function.WordCountMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @fileName: UseWindowFunction.java
 * @description: 使用window函数
 * @author: by echo huang
 * @date: 2020-08-30 15:47
 */
public class UseWindowFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> datasource = env.socketTextStream("hadoop", 9999);

        datasource.flatMap(new SplitFunction())
                .map(new WordCountMapFunction())
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                        return stringIntegerTuple2.f0;
                    }
                    // 滚动窗口
//                }).timeWindow(Time.seconds(15))
                    //滚动窗口，兼容UTC时区
//                }).timeWindow(Time.seconds(15), Time.hours(-8))
                    // 滚动计数窗口，底层依赖于全局窗口，触发器和移除器
//                }).countWindow(10)
                    // 会话窗口
//                }).window(EventTimeSessionWindows.withGap(Time.seconds(10)))
                    // 全局窗口
                }).window(GlobalWindows.create())
                .reduce(new SumReduceFunction())
                .print("window");


        env.execute();
    }
}
