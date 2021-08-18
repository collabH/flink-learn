package dev.learn.flink.feature;

import com.sun.tools.corba.se.idl.constExpr.Times;
import com.sun.xml.internal.ws.util.StreamUtils;
import dev.learn.flink.FlinkEnvUtils;
import dev.learn.flink.state.CustomStateSerialization;
import jdk.internal.util.EnvUtils;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.queryablestate.client.QueryableStateClient;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * @fileName: StateFeature.java
 * @description: 1.13状态
 * @author: huangshimin
 * @date: 2021/8/14 7:33 下午
 */
public class StateFeature {
    public static void main(String[] args) {
    }

    public static void keyedState() {
        StreamExecutionEnvironment streamEnv = FlinkEnvUtils.getStreamEnv();
        DataStreamSource<WordCount> ds = streamEnv.fromElements(WordCount.builder().word("hello")
                .id(1).build(), WordCount.builder().id(2).word("world").build());
        KeyedStream<WordCount, Integer> keyedStream = ds.keyBy(new KeySelector<WordCount, Integer>() {
            @Override
            public Integer getKey(WordCount wordCount) throws Exception {
                return wordCount.getId();
            }
        });
        keyedStream.process(new KeyedStateProcessFunction());
    }

    /**
     * 算子状态
     */
    public static void operatorState() {
        StreamExecutionEnvironment streamEnv = FlinkEnvUtils.getStreamEnv();
        DataStreamSource<Tuple2<String, Integer>> ds = streamEnv.fromElements(Tuple2.<String, Integer>of("name", 1));
        ds.addSink(new OperatorBufferingSink(10));
    }

    /**
     * 广播状态
     * <p>
     * 1.广播状态不会跨task读取，所以广播状态需要在process中进行更新，抱着每个task搜到广播数据后及时更新到广播状态中
     * 2.所有task都会对广播状态进行checkpoint这样是为了恢复状态时不会存在热点文件问题，但是存在写放大，根据p（并行度）的大小方法p倍
     * 3.  broadcast state 在运行时保存在内存中，需要保证内存充足
     */
    public static void broadcastState() {
        StreamExecutionEnvironment streamEnv = FlinkEnvUtils.getStreamEnv();
        MapStateDescriptor<String, String> broadcastStateDesc = new MapStateDescriptor<String, String>(
                // BasicTypeInfo继承TypeInformation的基础类型封装
                "broadcastDesc", BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(String.class));
        DataStreamSource<Integer> ds = streamEnv.fromElements(1, 2, 3, 4, 5, 6);
        DataStreamSource<Integer> broadcastDs = streamEnv.fromElements(1, 2);
        BroadcastStream<Integer> broadcast = broadcastDs.broadcast(broadcastStateDesc);
        ds.connect(broadcast)
                .process(new BroadcastProcessFunction<Integer, Integer, Integer>() {

                    /**
                     * 只能读取广播状态
                     * @param integer
                     * @param readOnlyContext
                     * @param collector
                     * @throws Exception
                     */
                    @Override
                    public void processElement(Integer integer, ReadOnlyContext readOnlyContext,
                                               Collector<Integer> collector) throws Exception {
                        ReadOnlyBroadcastState<String, String> broadcastState =
                                readOnlyContext.getBroadcastState(broadcastStateDesc);

                        String sum = broadcastState.get("sum");
                        collector.collect(Integer.parseInt(sum) + integer);
                    }

                    /**
                     * 广播状态更新
                     * @param integer
                     * @param context
                     * @param collector
                     * @throws Exception
                     */
                    @Override
                    public void processBroadcastElement(Integer integer, Context context,
                                                        Collector<Integer> collector) throws Exception {
                        BroadcastState<String, String> broadcastState = context.getBroadcastState(broadcastStateDesc);
                        broadcastState.put("sum", integer.toString());
                    }
                });
    }

    /**
     * 状态查询客户端
     * 将 flink-queryable-state-runtime_2.11-1.13.0.jar 从 Flink distribution 的 opt/ 目录拷贝到 lib/ 目录；
     * 将参数 queryable-state.enable 设置为 true
     */
    public static void queryStateClient() {
        StreamExecutionEnvironment streamEnv = FlinkEnvUtils.getStreamEnv();
        DataStreamSource<WordCount> ds = streamEnv.fromElements(WordCount.builder().word("hello")
                .id(1).build(), WordCount.builder().id(2).word("world").build());
        KeyedStream<WordCount, Integer> keyedStream = ds.keyBy(new KeySelector<WordCount, Integer>() {
            @Override
            public Integer getKey(WordCount wordCount) throws Exception {
                return wordCount.getId();
            }
        });
        keyedStream.process(new KeyedProcessFunction<Integer, WordCount, Object>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                ValueStateDescriptor<Long> valueStateDescriptor = new ValueStateDescriptor<>("test", Long.class);
                valueStateDescriptor.setQueryable("test-query");
            }

            @Override
            public void processElement(WordCount wordCount, Context context, Collector<Object> collector) throws Exception {

            }
        });
        ValueStateDescriptor<Long> valueStateDescriptor = new ValueStateDescriptor<>("test", Long.class);
        try {
            QueryableStateClient client = new QueryableStateClient("localhost", 42931);
            CompletableFuture<ValueState<Long>> kvState = client.getKvState(JobID.generate(), "test-query", "1",
                    TypeInformation.of(String.class),
                    valueStateDescriptor);
            ValueState<Long> longValueState = kvState.get();
            System.out.println("state:" + longValueState.value());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void customStateSerializer() {
        ListStateDescriptor<Tuple2<String, Integer>> descriptor =
                new ListStateDescriptor<>(
                        "state-name",
                        new CustomStateSerialization());
    }


    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    static class WordCount {
        private int id;
        private String word;
    }

    /**
     * keyedState
     */
    static class KeyedStateProcessFunction extends KeyedProcessFunction<Integer, WordCount, String> {
        private transient ValueState<Long> counterValueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ValueStateDescriptor<Long> counterValueStateDesc = new ValueStateDescriptor<>(
                    "counter", TypeInformation.of(Long.class)
            );
            this.counterValueState = getRuntimeContext().getState(counterValueStateDesc);
            // 暂时只支持processing time
            // heap statebackend 会专门存储时间戳  rocksDb会多8字节存储时间戳。
            StateTtlConfig stateTtlConfig = StateTtlConfig
                    .newBuilder(Time.seconds(1))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    // 修改策略：1.OnCreateAndWrite仅在创建和写入时更新 2.OnReadAndWrite:读取时也更新
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    // 数据在过期但还未被清理时的可见性：1.NeverReturnExpired - 不返回过期数据 2.ReturnExpiredIfNotCleanedUp - 会返回过期但未清理的数据
                    .disableCleanupInBackground()
                    // 关闭stateBackend支持的后台清理过期数据线程
                    .cleanupFullSnapshot()
                    // 启用全量快照时进行清理的策略，这可以减少整个快照的大小。当前实现中不会清理本地的状态，但从上次快照恢复时，不会恢复那些已经删除的过期数据。
                    .cleanupIncrementally(10, true)
                    // 增量式清理状态数据，在状态访问或/和处理时进行。如果某个状态开启了该清理策略，则会在存储后端保留一个所有状态的惰性全局迭代器。 每次触发增量清理时，从迭代器中选择已经过期的数进行清理。
                    .build();


            counterValueStateDesc.enableTimeToLive(stateTtlConfig);

        }

        @Override
        public void processElement(WordCount wordCount, Context context, Collector<String> collector) throws Exception {
            Long value = counterValueState.value();
            if (value == null) {
                value = 1L;
                counterValueState.update(value);
            }
            counterValueState.update(value + 1);
        }
    }

    /**
     * operatorState
     * 算子状态(或非键态)是绑定到一个并行操作符实例的状态。
     * Kafka连接器是在Flink中使用Operator State的一个很好的例子。Kafka消费者的每个并行实例都维护一个主题分区和偏移量的映射，作为它的操作符状态。
     * 算子状态接口支持在并行性改变时在并行操作符实例之间重新分配状态。
     */
    static class OperatorBufferingSink
            implements SinkFunction<Tuple2<String, Integer>>,
            CheckpointedFunction {

        private final int threshold;

        private transient ListState<Tuple2<String, Integer>> checkpointedState;

        private List<Tuple2<String, Integer>> bufferedElements;

        public OperatorBufferingSink(int threshold) {
            this.threshold = threshold;
            this.bufferedElements = new ArrayList<>();
        }

        @Override
        public void invoke(Tuple2<String, Integer> value, Context contex) throws Exception {
            bufferedElements.add(value);
            if (bufferedElements.size() == threshold) {
                for (Tuple2<String, Integer> element : bufferedElements) {
                    // send it to the sink
                }
                bufferedElements.clear();
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            checkpointedState.clear();
            for (Tuple2<String, Integer> element : bufferedElements) {
                checkpointedState.add(element);
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor<Tuple2<String, Integer>> descriptor =
                    new ListStateDescriptor<>(
                            "buffered-elements",
                            TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
                            }));

            checkpointedState = context.getOperatorStateStore().getListState(descriptor);

            if (context.isRestored()) {
                for (Tuple2<String, Integer> element : checkpointedState.get()) {
                    bufferedElements.add(element);
                }
            }
        }
    }
}
