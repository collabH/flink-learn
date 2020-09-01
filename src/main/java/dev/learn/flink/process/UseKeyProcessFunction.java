package dev.learn.flink.process;

import dev.learn.flink.domain.MessageEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

/**
 * @fileName: UseKeyProcessFunction.java
 * @description: use key processFunction
 * @author: by echo huang
 * @date: 2020/9/1 9:55 下午
 */
public class UseKeyProcessFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<MessageEvent> eventSource = env.socketTextStream("hadoop", 9999)
                .map(new MapFunction<String, MessageEvent>() {
                    @Override
                    public MessageEvent map(String value) throws Exception {
                        String[] arr = value.split(",");
                        return new MessageEvent(arr[0], arr[1], arr[2]);
                    }
                });
        OutputTag<Tuple2<String, String>> outputTag = new OutputTag<>("side-output");

        eventSource.keyBy(new KeySelector<MessageEvent, String>() {
            @Override
            public String getKey(MessageEvent value) throws Exception {
                return value.getId();
            }
        }).process(new AlarmProcessFunction(5000L, outputTag))
                .print();

        eventSource.getSideOutput(outputTag)
                .print();

        env.execute();
    }
}

