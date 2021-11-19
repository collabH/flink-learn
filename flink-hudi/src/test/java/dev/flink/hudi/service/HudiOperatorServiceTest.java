package dev.flink.hudi.service;

import com.google.common.collect.Lists;
import dev.flink.hudi.config.FlinkEnvConfig;
import dev.flink.hudi.constants.OperatorEnums;
import dev.flink.hudi.service.sql.SQLHudiOperatorService;
import dev.flink.hudi.service.sql.SQLOperator;
import dev.hudi.HudiSqlConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.Test;
import scala.runtime.Statics;

import java.util.function.Consumer;

/**
 * @fileName: HudiOperatorServiceTest.java
 * @description: hudi操作服务测试类
 * @author: huangshimin
 * @date: 2021/11/18 8:38 下午
 */
public class HudiOperatorServiceTest {

    private HudiOperatorService<StreamTableEnvironment, SQLOperator,
            Consumer<Row>> hudiOperatorService = new SQLHudiOperatorService();


    /**
     * "CREATE TABLE t1(\n" +
     * "  uuid VARCHAR(20),\n" +
     * "  name VARCHAR(10),\n" +
     * "  age INT,\n" +
     * "  ts TIMESTAMP(3),\n" +
     * "  `partition` VARCHAR(20)\n" +
     * ")\n" +
     * "PARTITIONED BY (`partition`)\n" +
     * "WITH (\n" +
     * "  'connector' = 'hudi',\n" +
     * "  '" + FlinkOptions.PATH.key() + "' = 'hdfs://user/flink/t1',\n" +
     * "  '" + FlinkOptions.TABLE_TYPE.key() + "' = '" + HoodieTableType.MERGE_ON_READ
     * .name() + "',\n" +
     * " '" + FlinkOptions.COMPACTION_TRIGGER_STRATEGY.key() + "'='" + FlinkOptions
     * .NUM_COMMITS + "',\n" +
     * " '" + FlinkOptions.ARCHIVE_MAX_COMMITS.key() + "'='" + 30 + "',\n" +
     * " '" + FlinkOptions.ARCHIVE_MIN_COMMITS.key() + "'='" + 20 + "',\n" +
     * " '" + FlinkOptions.BUCKET_ASSIGN_TASKS.key() + "'='" + cores + "',\n" +
     * " '" + FlinkOptions.CLEAN_ASYNC_ENABLED.key() + "'='" + false + "',\n" +
     * " '" + FlinkOptions.CLEAN_RETAIN_COMMITS.key() + "'='" + 10 + "',\n" +
     * " '" + FlinkOptions.COMPACTION_ASYNC_ENABLED.key() + "'='" + true + "',\n" +
     * " '" + FlinkOptions.COMPACTION_DELTA_COMMITS.key() + "'='" + 5 + "',\n" +
     * " '" + FlinkOptions.COMPACTION_SCHEDULE_ENABLED.key() + "'='" + true + "',\n" +
     * " '" + FlinkOptions.COMPACTION_TASKS.key() + "'='" + 20 + "',\n" +
     * " '" + FlinkOptions.COMPACTION_MAX_MEMORY.key() + "'='" + 200 + "',\n" +
     * " '" + FlinkOptions.WRITE_TASKS.key() + "'='" + cores + "',\n" +
     * //                        " '" + FlinkOptions.WRITE_TASK_MAX_SIZE.key() + "'='" + cores + "',\n" +
     * " '" + FlinkOptions.WRITE_BATCH_SIZE.key() + "'='" + 128D + "',\n" +
     * " '" + FlinkOptions.TABLE_NAME.key() + "'='t1',\n" +
     * " '" + FlinkOptions.PRECOMBINE_FIELD.key() + "'='uuid',\n" +
     * " '" + FlinkOptions.PARTITION_PATH_FIELD.key() + "'='partition',\n" +
     * " '" + FlinkOptions.OPERATION.key() + "'='upsert',\n" +
     * " '" + FlinkOptions.METADATA_ENABLED.key() + "'='true'\n" +
     * //                        " '" + FlinkOptions.WRITE_BULK_INSERT_SHUFFLE_BY_PARTITION.key() + "'='" +
     * cores + "',
     * //                        \n" +
     * //                        " '" + FlinkOptions.WRITE_BULK_INSERT_SORT_BY_PARTITION.key() + "'='" + cores +
     * "',\n" +
     * //                        " '" + FlinkOptions.WRITE_COMMIT_ACK_TIMEOUT.key() + "'='" + cores + "',\n" +
     * //                        " '" + FlinkOptions.WRITE_LOG_BLOCK_SIZE.key() + "'='" + cores + "',\n" +
     * //                        " '" + FlinkOptions.WRITE_LOG_MAX_SIZE.key() + "'='" + cores + "',\n" +
     * //                        " '" + FlinkOptions.WRITE_MERGE_MAX_MEMORY.key() + "'='" + cores + "',\n" +
     * //                        " '" + FlinkOptions.WRITE_RATE_LIMIT.key() + "'='" + cores + "',\n" +
     * //                        " '" + FlinkOptions.WRITE_SORT_MEMORY.key() + "'='" + cores + "',\n" +
     * ");"
     */
    @Test
    public void testFlinkSQLOnHudi() {

        StreamTableEnvironment streamTableEnv = FlinkEnvConfig.getStreamTableEnv();
        int cores = Runtime.getRuntime().availableProcessors();
        long ts = System.currentTimeMillis();
        String columns = "id int,age int,name string,create_time bigint,update_time bigint,dt string";
        String insertSQL = HudiSqlConfig.getDML(OperatorEnums.INSERT, columns, "hudi_user", null,
                "VALUES(1,23,'hsm'," + ts + "," + ts + ",'202109')");
        SQLOperator sqlOperator = SQLOperator.builder()
                .ddlSQLList(Lists.newArrayList(HudiSqlConfig.getDDL(cores, "hudi_user", columns, "id",
                        "update_time", "dt")))
                .coreSQLList(Lists.newArrayList(insertSQL, "select * from hudi_user"))
                .build();
        hudiOperatorService.operation(streamTableEnv, sqlOperator, new Consumer<Row>() {
            @Override
            public void accept(Row row) {
                System.out.println(row);
            }
        });
    }


}
