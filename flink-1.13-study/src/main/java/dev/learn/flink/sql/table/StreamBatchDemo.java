package dev.learn.flink.sql.table;

import dev.learn.flink.FlinkEnvUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.connector.datagen.table.DataGenConnectorOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @fileName: StreamBatchDemo.java
 * @description: 流批一体
 * @author: huangshimin
 * @date: 2022/12/21 8:27 PM
 */
public class StreamBatchDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment streamEnv = FlinkEnvUtils.getStreamEnv();
        streamEnv.setRuntimeMode(RuntimeExecutionMode.STREAMING);
//        streamEnv.setRuntimeMode(RuntimeExecutionMode.BATCH);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);

        tableEnv.createTemporaryTable("tempSourceTable", TableDescriptor.forConnector("datagen")
                .schema(Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .build())
                .option(DataGenConnectorOptions.ROWS_PER_SECOND, 100L)
                .build());

        tableEnv.from("tempSourceTable").execute().print();
    }
}
