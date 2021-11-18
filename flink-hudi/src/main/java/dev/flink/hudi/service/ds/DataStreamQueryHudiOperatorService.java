package dev.flink.hudi.service.ds;

import dev.flink.hudi.service.HudiOperatorService;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.source.StreamReadMonitoringFunction;
import org.apache.hudi.source.StreamReadOperator;
import org.apache.hudi.table.HoodieTableSource;
import org.apache.hudi.table.format.FilePathUtils;
import org.apache.hudi.table.format.mor.MergeOnReadInputFormat;
import org.apache.hudi.table.format.mor.MergeOnReadInputSplit;

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
    public void operation(StreamExecutionEnvironment streamExecutionEnvironment, DataStreamOperator dataStreamOperator,
                          Consumer<String> collector) {
        dataStreamOperator.checkParams();
        // 看HoodieTableSource#produceDataStream
    }
}
