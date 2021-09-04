package dev.learn.flink.feature;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.twitter.chill.protobuf.ProtobufSerializer;
import com.twitter.chill.thrift.TBaseSerializer;
import dev.learn.flink.FlinkEnvUtils;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @fileName: SupportDataType.java
 * @description: Flink支持的数据类型
 * @author: huangshimin
 * @date: 2021/9/4 5:01 下午
 */
public class SupportDataType {
    public static void main(String[] args) {
        // tuple
        StreamExecutionEnvironment streamEnv = FlinkEnvUtils.getStreamEnv();
        DataStreamSource<Tuple1<Integer>> tupleStream = streamEnv.fromElements(Tuple1.of(1));

        DataStream<WordWithCount> wordCounts = streamEnv.fromElements(
                new WordWithCount("hello", 1),
                new WordWithCount("world", 2));
        // 注册自定义kryo序列化器
//        streamEnv.getConfig().registerTypeWithKryoSerializer(WordWithCount.class, new Serializer<>() {
//            @Override
//            public void write(Kryo kryo, Output output, Object o) {
//                kryo.writeObject(output, o);
//            }
//
//            @Override
//            public Object read(Kryo kryo, Input input, Class aClass) {
//                return kryo.readClassAndObject(input);
//            }
//        });

        // 注册google protobu序列号器
        streamEnv.getConfig().registerTypeWithKryoSerializer(WordWithCount.class, ProtobufSerializer.class);
// 注册 Apache Thrift 序列化器为标准序列化器
// TBaseSerializer 需要初始化为默认的 kryo 序列化器
        streamEnv.getConfig().addDefaultKryoSerializer(WordWithCount.class, TBaseSerializer.class);

    }

    public static class WordWithCount {

        public String word;
        public int count;

        public WordWithCount() {
        }

        public WordWithCount(String word, int count) {
            this.word = word;
            this.count = count;
        }
    }
}
