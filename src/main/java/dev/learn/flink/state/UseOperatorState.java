package dev.learn.flink.state;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

/**
 * @fileName: UseOperatorState.java
 * @description: 使用算子状态
 * @author: by echo huang
 * @date: 2020/9/1 11:11 上午
 */
public class UseOperatorState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        DataStreamSource<Integer> datasource = env.fromElements(1, 2, 3, 4, 5, 6);
        datasource.map(new StateMapFunction(2)).print();

        env.execute();
    }
}

class StateMapFunction implements MapFunction<Integer, String>, CheckpointedFunction {

    private final Integer threshold;

    private transient ListState<String> listState;

    private List<String> bufferData = Lists.newArrayList();

    public StateMapFunction(Integer threshold) {
        this.threshold = threshold;
    }

    @Override
    public String map(Integer value) throws Exception {
        if (bufferData.size() == threshold) {
            for (String bufferDatum : bufferData) {
                // sink
                System.out.println(bufferDatum);
            }
            bufferData.clear();
        }
        bufferData.add("operator:" + value);
        return "isEmpty:" + bufferData.isEmpty();
    }

    /**
     * 每次checkpoint时调用
     *
     * @param functionSnapshotContext
     * @throws Exception
     */
    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        listState.clear();
        for (String bufferDatum : bufferData) {
            listState.add(bufferDatum);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        // 初始化状态
        ListStateDescriptor<String> descriptor =
                new ListStateDescriptor<>(
                        "buffered-elements", String.class);
        listState = functionInitializationContext.getOperatorStateStore().getListState(descriptor);
        // 从checkpoint中恢复状态
        if (functionInitializationContext.isRestored()) {
            for (String data : listState.get()) {
                bufferData.add(data);
            }
        }
    }
}