package dev.learn.flink.join;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @fileName: UseWindowJoin.java
 * @description: 使用 window join
 * @author: by echo huang
 * @date: 2020/9/2 10:18 上午
 */
public class UseWindowJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // stream 1
        DataStreamSource<String> stream1 = env.socketTextStream("hadoop", 9999);
        DataStreamSource<String> stream2 = env.socketTextStream("hadoop", 10000);

        SingleOutputStreamOperator<Tuple2<String, String>> parseStream1 = stream1.map(new ParseFunction());

        SingleOutputStreamOperator<Tuple2<String, String>> parseStream2 = stream2.map(new ParseFunction());


        parseStream1.join(parseStream2)
                .where(new KeySelector<Tuple2<String, String>, String>() {
                    @Override
                    public String getKey(Tuple2<String, String> value) throws Exception {
                        return value.f0;
                    }
                }).equalTo(new KeySelector<Tuple2<String, String>, String>() {
            @Override
            public String getKey(Tuple2<String, String> value) throws Exception {
                return value.f0;
            }
        }).window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.seconds(5))
                .apply(new JoinFunction<Tuple2<String, String>, Tuple2<String, String>, String>() {
                    @Override
                    public String join(Tuple2<String, String> first, Tuple2<String, String> second) throws Exception {
                        return first.f1 + "," + second.f1;
                    }
                }).print();


        env.execute();

    }
}

/**
 * 解析函数
 */
class ParseFunction implements MapFunction<String, Tuple2<String, String>> {

    @Override
    public Tuple2<String, String> map(String value) throws Exception {
        String[] arr = value.split(",");
        return Tuple2.of(arr[0], arr[1]);
    }
}
