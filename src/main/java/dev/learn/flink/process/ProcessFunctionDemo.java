package dev.learn.flink.process;

import dev.learn.flink.domain.MessageEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @fileName: ProcessFunctionDemo.java
 * @description: ProcessFunctionDemo.java类说明
 * @author: by echo huang
 * @date: 2021/3/18 9:44 下午
 */
public class ProcessFunctionDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<MessageEvent> eventSource = env.socketTextStream("hadoop", 9999)
                .map(new MapFunction<String, MessageEvent>() {
                    @Override
                    public MessageEvent map(String value) throws Exception {
                        String[] arr = value.split(",");
                        return new MessageEvent(arr[0], arr[1], arr[2]);
                    }
                });

        SingleOutputStreamOperator<String> keyedProcessSource = eventSource.keyBy(new KeySelector<MessageEvent, String>() {
            @Override
            public String getKey(MessageEvent value) throws Exception {
                return value.getId();
            }
        }).process(new KeyedProcessFunction<String, MessageEvent, String>() {
            @Override
            public void processElement(MessageEvent value, Context ctx, Collector<String> out) throws Exception {
                out.collect("test keyed process");
            }
        });

        // coProcess
        eventSource.connect(eventSource)
                .process(new CoProcessFunction<MessageEvent, MessageEvent, String>() {
                    @Override
                    public void processElement1(MessageEvent value, Context ctx, Collector<String> out) throws Exception {
                        out.collect("processElement1");
                    }

                    @Override
                    public void processElement2(MessageEvent value, Context ctx, Collector<String> out) throws Exception {
                        out.collect("processElement2");
                    }
                });

        // broadcast
        MapStateDescriptor<String, MessageEvent> broadcastState= new MapStateDescriptor<String, MessageEvent>("broadcast", TypeInformation.of(String.class),
                TypeInformation.of(MessageEvent.class));
        BroadcastStream<MessageEvent> broadcast = eventSource.broadcast(broadcastState);

        eventSource.connect(broadcast)
                .process(new BroadcastProcessFunction<MessageEvent, MessageEvent, String>() {
                    @Override
                    public void processElement(MessageEvent value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                        out.collect("process datasource");
                    }

                    @Override
                    public void processBroadcastElement(MessageEvent value, Context ctx, Collector<String> out) throws Exception {
                        out.collect("process broadcast datasource");
                    }
                });




    }
}
