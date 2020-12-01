package dev.learn.flink.join;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.CoGroupedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.JoinedStreams;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.table.runtime.operators.window.CountWindow;
import org.apache.flink.util.Collector;

/**
 * @fileName: JoinCoGroupDemo.java
 * @description: JoinCoGroupDemo.java类说明
 * @author: by echo huang
 * @date: 2020/12/1 10:55 下午
 */
public class JoinCoGroupDemo {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<Integer, String>> data1 = env.fromElements(Tuple2.of(1, "a"), Tuple2.of(2, "b"));
        DataStreamSource<Tuple2<Integer, String>> data2 = env.fromElements(Tuple2.of(1, "c"), Tuple2.of(3, "d"));

        data1.coGroup(data2)
                .where(new KeySelector<Tuple2<Integer, String>, Integer>() {
                    @Override
                    public Integer getKey(Tuple2<Integer, String> value) throws Exception {
                        return value.f0;
                    }
                }).equalTo(new KeySelector<Tuple2<Integer, String>, Integer>() {
                    @Override
                    public Integer getKey(Tuple2<Integer, String> value) throws Exception {
                        return value.f0;
                    }
                }).window(GlobalWindows.create())
                .trigger(CountTrigger.of(1))
                .apply(new CoGroupFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, String>() {
                    @Override
                    public void coGroup(Iterable<Tuple2<Integer, String>> first, Iterable<Tuple2<Integer, String>> second, Collector<String> out) throws Exception {

                    }
                });

         data1.join(data2)
                .where(new KeySelector<Tuple2<Integer, String>, Integer>() {
                    @Override
                    public Integer getKey(Tuple2<Integer, String> value) throws Exception {
                        return value.f0;
                    }
                }).equalTo(new KeySelector<Tuple2<Integer, String>, Integer>() {
                    @Override
                    public Integer getKey(Tuple2<Integer, String> value) throws Exception {
                        return value.f0;
                    }
                }).window(GlobalWindows.create())
                .trigger(CountTrigger.of(1))
                .apply(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, String>() {
                    @Override
                    public String join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        return null;
                    }
                });
    }
}
