package dev.flink.hudi.service.ds;

import dev.flink.hudi.service.HudiOperatorService;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.function.Consumer;


/**
 * @fileName: DataStreamHudiOperatorService.java
 * @description: dataStream操作hudi
 * @author: huangshimin
 * @date: 2021/11/18 5:28 下午
 */
public class DataStreamQueryHudiOperatorService implements HudiOperatorService<StreamExecutionEnvironment,
        DataStreamOperator, Consumer<String>> {

    @Override
    public void operation(StreamExecutionEnvironment streamExecutionEnvironment,
                          DataStreamOperator dataStreamOperator,
                          Consumer<String> collector) {
        dataStreamOperator.checkParams();
        // 看HoodieTableSource#produceDataStream

    }
}
