package dev.flink.hudi.service.ds;

import dev.flink.hudi.service.HudiOperatorService;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.hudi.util.HoodiePipeline;

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
        HoodiePipeline.Builder builder = HoodiePipeline.builder(dataStreamOperator.getTargetTable());
        for (String column : dataStreamOperator.getColumns()) {
            builder.column(column);
        }
        builder.pk(dataStreamOperator.getPk().toArray(new String[]{}))
                .partition(dataStreamOperator.getPartition().toArray(new String[]{}))
                .options(dataStreamOperator.getTableOptions())
                .source(streamExecutionEnvironment).print();
        try {
            streamExecutionEnvironment.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
