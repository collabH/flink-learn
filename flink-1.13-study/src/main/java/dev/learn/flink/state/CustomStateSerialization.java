package dev.learn.flink.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * @fileName: CustomStateSerialization.java
 * @description: 自定义状态序列化
 * @author: huangshimin
 * @date: 2021/8/18 10:26 下午
 */
public class CustomStateSerialization extends TypeSerializer<Tuple2<String, Integer>> {
    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<Tuple2<String, Integer>> duplicate() {
        return null;
    }

    @Override
    public Tuple2<String, Integer> createInstance() {
        return null;
    }

    @Override
    public Tuple2<String, Integer> copy(Tuple2<String, Integer> from) {
        return null;
    }

    @Override
    public Tuple2<String, Integer> copy(Tuple2<String, Integer> from, Tuple2<String, Integer> reuse) {
        return null;
    }

    @Override
    public int getLength() {
        return 0;
    }

    @Override
    public void serialize(Tuple2<String, Integer> record, DataOutputView target) throws IOException {

    }

    @Override
    public Tuple2<String, Integer> deserialize(DataInputView source) throws IOException {
        return null;
    }

    @Override
    public Tuple2<String, Integer> deserialize(Tuple2<String, Integer> reuse, DataInputView source) throws IOException {
        return null;
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {

    }

    @Override
    public boolean equals(Object obj) {
        return false;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public TypeSerializerSnapshot<Tuple2<String, Integer>> snapshotConfiguration() {
        return null;
    }
}
