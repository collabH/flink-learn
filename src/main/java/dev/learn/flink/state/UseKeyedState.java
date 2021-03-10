package dev.learn.flink.state;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * @fileName: UseKeyedState.java
 * @description: 使用键控状态
 * @author: by echo huang
 * @date: 2020/8/31 10:57 下午
 */
public class UseKeyedState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<String, Integer>> datasource = env.fromElements(Tuple2.of("a", 1), Tuple2.of("a", 2), Tuple2.of("a", 3));

        // vlaue state
        datasource
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                })
                .map(new RichMapFunction<Tuple2<String, Integer>, Integer>() {
                    private transient ValueState<Integer> count;
                    private transient ReducingState<String> reducingState;
                    private transient MapState<String, String> mapState;
                    private transient ListState<String> listState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<Integer> countDescriptor = new ValueStateDescriptor<Integer>("value-count", Integer.class);
                        count = getRuntimeContext().getState(countDescriptor);
                        ReducingStateDescriptor<String> reducingStateDescriptor = new ReducingStateDescriptor<String>(
                                "reducing", new ReduceFunction<String>() {
                            @Override
                            public String reduce(String value1, String value2) throws Exception {
                                return value1 + value2;
                            }
                        }, String.class);
                        StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.of(1, TimeUnit.HOURS)).build();
                        MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, String.class);
                        // 设置ttl state
                        mapStateDescriptor.enableTimeToLive(stateTtlConfig);
                        mapState = getRuntimeContext().getMapState(mapStateDescriptor);
                        reducingState = getRuntimeContext().getReducingState(reducingStateDescriptor);
                        super.open(parameters);
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                        // clean value state
                        count.clear();
                    }


                    @Override
                    public Integer map(Tuple2<String, Integer> value) throws Exception {
                        // 第一次修改
                        if (count.value() == null) {
                            count.update(value.f1);
                        }
                        System.out.println(count.value());
                        return value.f1;
                    }
                }).print();

        env.execute();
    }
}
