package dev.learn.flink.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * @fileName: UseStateful.java
 * @description: UseStateful.java类说明
 * @author: by echo huang
 * @date: 2020/8/31 11:34 下午
 */
public class UseStateful {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<Event> datasource = env.addSource(new SourceFunction<Event>() {
            @Override
            public void run(SourceContext<Event> sourceContext) throws Exception {

                int c = 0;
                while (true) {
                    int id = new Random().nextInt(10);
                    sourceContext.collect(new Event(id, c, System.currentTimeMillis() / 1000));
                    c += new Random().nextInt(30);
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {

            }
        });

        datasource.keyBy(new KeySelector<Event, Integer>() {
            @Override
            public Integer getKey(Event value) throws Exception {
                return value.getId();
            }
        }).flatMap(new RichFlatMapFunction<Event, String>() {
            private transient ValueState<Integer> lastTempState;

            private transient ValueState<Integer> ttlState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<Integer> valueStateDescriptor = new ValueStateDescriptor<>("count", Integer.class);
                lastTempState = getRuntimeContext().getState(valueStateDescriptor);
                // ttl state
                StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.seconds(10))
                        .neverReturnExpired()
                        .updateTtlOnCreateAndWrite()
                        .cleanupFullSnapshot()
                        .build();
                ValueStateDescriptor<Integer> ttlStateDescriptor = new ValueStateDescriptor<>("ttl-state", Integer.class);
                // set up ttl
                ttlStateDescriptor.enableTimeToLive(ttlConfig);
                ttlState = getRuntimeContext().getState(ttlStateDescriptor);
            }

            @Override
            public void flatMap(Event value, Collector<String> out) throws Exception {

                // 当天温度值
                Integer currentTemp = value.getC();
                if (lastTempState.value() != null) {
                    // 上次温度值
                    int lastTemp = lastTempState.value();
                    int diff = Math.abs(currentTemp - lastTemp);
                    // 差值大于10 输出
                    if (diff > 10) {
                        out.collect(value.getId() + ":" + value.getC() + ":" + value.getTimestamp() + "差值异常");
                    }
                }
                if (ttlState.value() != null) {
                    System.out.println(ttlState.value());
                }
                ttlState.update(1);
                lastTempState.update(currentTemp);
            }
        }).print();

        env.execute();
    }
}

class Event {
    public Event() {
    }


    private Integer id;
    private Integer c;
    private Long timestamp;

    public Event(Integer id, Integer c, Long timestamp) {
        this.id = id;
        this.c = c;
        this.timestamp = timestamp;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getC() {
        return c;
    }

    public void setC(Integer c) {
        this.c = c;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
}
