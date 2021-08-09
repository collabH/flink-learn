package dev.learn.flink.feature;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * @fileName: ExecutionMode.java
 * @description: ExecutionMode.java类说明
 * @author: huangshimin
 * @date: 2021/8/8 8:35 下午
 */
public class ExecutionModeFeature {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
//        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
//        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        // 执行DAG不同，批处理可以更多地优化

        // 状态后端 1.流模式flink使用状态后端控制状态如何存储如何做ck  2.批处理忽略状态后端，相反，键操作的输入按键分组(使用排序)
        // ，然后依次处理键的所有记录。这允许同时只保留一个键的状态。当移动到下一个键时，给定键的状态将被丢弃。

        // 2.处理顺序，流模式无法保证顺序一旦达到就处理除非配合watermark等， 批处理能够保证顺序

        // 3. 处理时间，Batch模式使用处理时间可以注册处理时间timers

        List<Result> inputs = Lists.newArrayList();
        long ts = System.currentTimeMillis();
        inputs.add(Result.builder().ts(ts)
                .id("1").build());
        inputs.add(Result.builder().ts(ts + 5 * 1000)
                .id("1").build());
        inputs.add(Result.builder().ts(ts + 6 * 1000)
                .id("1").build());
        inputs.add(Result.builder().ts(ts + 7 * 1000)
                .id("1").build());
        inputs.add(Result.builder().ts(ts + 10 * 1000)
                .id("1").build());
        inputs.add(Result.builder().ts(ts + 15 * 1000)
                .id("1").build());
        DataStreamSource<Result> ds = env.fromCollection(inputs);
        registerBatchTimer(ds);

        env.execute();
    }

    private static void registerBatchTimer(DataStreamSource<Result> ds) {
        ds.keyBy(new KeySelector<Result, String>() {
            @Override
            public String getKey(Result result) throws Exception {
                return result.getId();
            }
        }).process(new KeyedProcessFunction<String, Result, String>() {
            private transient ValueStateDescriptor<Long> longValueStateDescriptor;
            private transient ValueState<Long> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                this.longValueStateDescriptor = new ValueStateDescriptor<Long>("timer",
                        TypeInformation.of(Long.class));
                valueState = getRuntimeContext().getState(this.longValueStateDescriptor);
            }

            @Override
            public void processElement(Result result, Context context, Collector<String> collector) throws Exception {
                TimerService timerService = context.timerService();
                Long ts = result.getTs();
                long timeTrigger = ts + 5000;
                if (valueState.value() == null || valueState.value() == -1L) {
                    timerService.registerProcessingTimeTimer(timeTrigger);
                    valueState.update(timeTrigger);
                }

            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                super.onTimer(timestamp, ctx, out);
                Long timer = valueState.value();
                ctx.timerService().deleteProcessingTimeTimer(timer);
                valueState.update(-1L);
                System.out.println("ts:" + timestamp);
            }
        });
    }


    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    static class Result {
        private String id;
        private Long ts;
    }
}
