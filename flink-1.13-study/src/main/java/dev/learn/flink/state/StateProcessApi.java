package dev.learn.flink.state;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.state.api.BootstrapTransformation;
import org.apache.flink.state.api.ExistingSavepoint;
import org.apache.flink.state.api.OperatorTransformation;
import org.apache.flink.state.api.Savepoint;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.state.api.functions.StateBootstrapFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * @fileName: StateProcessApi.java
 * @description: 状态处理API
 * @author: huangshimin
 * @date: 2021/9/5 7:36 下午
 */
public class StateProcessApi {

    /**
     * 使用状态处理API可以通过DataSet方式读写修改savepoint和checkpoint
     * <p>
     * 对于状态而言每个算子的UID是状态映射的namesapce，状态存储在这个命名空间里，按照对应的状态区分
     */

    public static void readState() throws Exception {
        ExecutionEnvironment bEnv = ExecutionEnvironment.getExecutionEnvironment();
        ExistingSavepoint savepoint = Savepoint.load(bEnv, "hdfs://flink/savepoints", new MemoryStateBackend());
        // 读取listState
        DataSource<Integer> listState = savepoint.readListState("my-uid", "name", TypeInformation.of(Integer.class));
        listState.print();
        //读取union状态
        savepoint.readUnionState("union-uid", "union", TypeInformation.of(Integer.class)).print();
        //读取broadCast状态
        savepoint.readBroadcastState("broadcast-uid", "broadcsat", TypeInformation.of(String.class),
                TypeInformation.of(Integer.class)).print();
        //读取自定义状态
        DataSet<Integer> customState = savepoint.readListState(
                "uid",
                "list-state",
                Types.INT, new TypeSerializer<Integer>() {
                    @Override
                    public boolean isImmutableType() {
                        return false;
                    }

                    @Override
                    public TypeSerializer<Integer> duplicate() {
                        return null;
                    }

                    @Override
                    public Integer createInstance() {
                        return null;
                    }

                    @Override
                    public Integer copy(Integer from) {
                        return null;
                    }

                    @Override
                    public Integer copy(Integer from, Integer reuse) {
                        return null;
                    }

                    @Override
                    public int getLength() {
                        return 0;
                    }

                    @Override
                    public void serialize(Integer record, DataOutputView target) throws IOException {

                    }

                    @Override
                    public Integer deserialize(DataInputView source) throws IOException {
                        return null;
                    }

                    @Override
                    public Integer deserialize(Integer reuse, DataInputView source) throws IOException {
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
                    public TypeSerializerSnapshot<Integer> snapshotConfiguration() {
                        return null;
                    }
                });

        savepoint.readKeyedState("keyed-uid", new ReaderFunction());
    }

    public static void writeState(){
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Integer> data = env.fromElements(1, 2, 3);
        //opeartor state
        BootstrapTransformation<Integer> transformation = OperatorTransformation
                .bootstrapWith(data)
                .transform(new SimpleBootstrapFunction());

        Savepoint.create(new MemoryStateBackend(),128)
                .withOperator("test",transformation)
                .write("hdfs://xxx/write");
    }

    public static void modityState() throws IOException {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Integer> data = env.fromElements(1, 2, 3);
        BootstrapTransformation<Integer> transformation = OperatorTransformation
                .bootstrapWith(data)
                .transform(new SimpleBootstrapFunction());
        Savepoint.load(env,"hdfs://old/ck",new MemoryStateBackend())
                .withOperator("test-id",transformation)
                .write("hdfs://new/ck");
    }
    public static class SimpleBootstrapFunction extends StateBootstrapFunction<Integer> {

        private ListState<Integer> state;

        @Override
        public void processElement(Integer value, Context ctx) throws Exception {
            state.add(value);
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            state = context.getOperatorStateStore().getListState(new ListStateDescriptor<>("state", Types.INT));
        }
    }

    public static class KeyedState {
        public int key;

        public int value;

        public List<Long> times;
    }

    public static class ReaderFunction extends KeyedStateReaderFunction<Integer, KeyedState> {

        ValueState<Integer> state;

        ListState<Long> updateTimes;

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("state", Types.INT);
            state = getRuntimeContext().getState(stateDescriptor);

            ListStateDescriptor<Long> updateDescriptor = new ListStateDescriptor<>("times", Types.LONG);
            updateTimes = getRuntimeContext().getListState(updateDescriptor);
        }

        @Override
        public void readKey(
                Integer key,
                Context ctx,
                Collector<KeyedState> out) throws Exception {

            KeyedState data = new KeyedState();
            data.key = key;
            data.value = state.value();
            data.times = StreamSupport
                    .stream(updateTimes.get().spliterator(), false)
                    .collect(Collectors.toList());

            out.collect(data);
        }
    }
}
