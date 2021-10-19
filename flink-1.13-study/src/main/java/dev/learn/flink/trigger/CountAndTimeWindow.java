package dev.learn.flink.trigger;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @fileName: CountAndTimeWindow.java
 * @description: CountAndTimeWindow.java类说明
 * @author: huangshimin
 * @date: 2021/10/19 2:39 下午
 */
public class CountAndTimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new TimeSourceFunction())
                .keyBy(new KeySelector<Tuple2<String, String>, String>() {
                    @Override
                    public String getKey(Tuple2<String, String> stringStringTuple2) throws Exception {
                        return stringStringTuple2.f0;
                    }
                }).window(TumblingProcessingTimeWindows.of(Time.milliseconds(400)))
                .trigger(CountTriggerWithTimeout.of(5, false))
                .process(new ProcessWindowFunction<Tuple2<String, String>, String
                        , String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<Tuple2<String, String>> iterable,
                                        Collector<String> collector) throws Exception {
                        for (Tuple2<String, String> stringStringTuple2 : iterable) {
                            System.out.println(stringStringTuple2.f0 + "=====" + stringStringTuple2.f1);
                        }
                        collector.collect(key);
                    }
                });
        env.execute();
    }
}
