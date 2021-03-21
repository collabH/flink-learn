package dev.learn.flink.window.function;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowStagger;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @fileName: WindowFunction.java
 * @description: WindowFunction.java类说明
 * @author: by echo huang
 * @date: 2021/3/21 10:06 下午
 */
public class WindowFunction {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        WindowedStream<Integer, Integer, TimeWindow> window = env.fromElements(1, 23, 4, 5, 6)
                .keyBy((KeySelector<Integer, Integer>) value -> value)
                .window(TumblingEventTimeWindows.of(Time.seconds(1), Time.hours(-8), WindowStagger.ALIGNED));
        // process
        window
                .process(new ProcessWindowFunction<Integer, Object, Integer, TimeWindow>() {
                    @Override
                    public void process(Integer integer, Context context, Iterable<Integer> elements, Collector<Object> out) throws Exception {
                        System.out.println("全量process窗口函数");
                    }
                });
        //agg
        window.aggregate(new AggregateFunction<Integer, String, String>() {
            @Override
            public String createAccumulator() {
                return null;
            }

            @Override
            public String add(Integer value, String accumulator) {
                return null;
            }

            @Override
            public String getResult(String accumulator) {
                return null;
            }

            @Override
            public String merge(String a, String b) {
                return null;
            }
        });

    }
}
