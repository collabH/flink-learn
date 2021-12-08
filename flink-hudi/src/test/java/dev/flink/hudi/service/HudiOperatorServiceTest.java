package dev.flink.hudi.service;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import dev.flink.hudi.builder.SqlBuilderFactory;
import dev.flink.hudi.builder.column.ColumnInfo;
import dev.flink.hudi.config.FlinkEnvConfig;
import dev.flink.hudi.constants.OperatorEnums;
import dev.flink.hudi.constants.SQLEngine;
import dev.flink.hudi.service.sql.SQLHudiOperatorService;
import dev.flink.hudi.service.sql.SQLOperator;
import dev.hudi.HudiSqlConfig;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.configuration.FlinkOptions;
import org.junit.Test;

import java.util.Map;
import java.util.function.Consumer;

/**
 * @fileName: HudiOperatorServiceTest.java
 * @description: hudi操作服务测试类
 * @author: huangshimin
 * @date: 2021/11/18 8:38 下午
 */
public class HudiOperatorServiceTest {

    private HudiOperatorService<StreamTableEnvironment, SQLOperator,
            Consumer<TableResult>> hudiOperatorService = new SQLHudiOperatorService();
    private String columns = "id int,age int,name string,create_time date,update_time date,dt string";
    private String sourceTableName = "source";
    private String sourceSQLDDL = HudiSqlConfig.getGeneratorSourceSQLDDL(sourceTableName, columns);


    @Test
    public void testFlinkSQLOnHudi() {

        StreamTableEnvironment streamTableEnv = FlinkEnvConfig.getStreamTableEnv();
        int cores = Runtime.getRuntime().availableProcessors();
        String columns = "id int,age int,name string,create_time date,update_time date,dt string";
        String sourceTableName = "source";
        String sinkTableName = "hudi_user";
        String sourceSQLDDL = HudiSqlConfig.getGeneratorSourceSQLDDL(sourceTableName, columns);
        String sinkSQLDDL = HudiSqlConfig.getDDL(cores, sinkTableName, columns, "id",
                "update_time", "dt", false);
        String insertSQLDML = HudiSqlConfig.getDML(OperatorEnums.INSERT, "*", sinkTableName, sourceTableName, "");
        SQLOperator sqlOperator = SQLOperator.builder()
                .ddlSQLList(Lists.newArrayList(sourceSQLDDL, sinkSQLDDL))
                .coreSQLList(Lists.newArrayList(insertSQLDML))
                .build();
        hudiOperatorService.operation(streamTableEnv, sqlOperator, new Consumer<TableResult>() {
            @Override
            public void accept(TableResult tableResult) {
                tableResult.print();
            }
        });
    }

    @Test
    public void printDDL() {
        int cores = Runtime.getRuntime().availableProcessors();
        String columns = "id int,age int,name string,create_time date,update_time date,dt string";
        String sourceTableName = "source";
        String sinkTableName = "hudi_user";
        String sourceSQLDDL = HudiSqlConfig.getGeneratorSourceSQLDDL(sourceTableName, columns);
        String sinkSQLDDL = HudiSqlConfig.getDDL(cores, sinkTableName, columns, "id",
                "update_time", "dt", false);
        System.out.println(sinkSQLDDL);
    }


    @Test
    public void testStreamingRead() throws InterruptedException {
        StreamTableEnvironment streamTableEnv = FlinkEnvConfig.getStreamTableEnv();
        int cores = Runtime.getRuntime().availableProcessors();
        String columns = "id int,age int,name string,create_time date,update_time date,dt string";
        String sourceTableName = "hudi_user";
        String sourceDDL = HudiSqlConfig.getDDL(cores, sourceTableName, columns, "id", "update_time", "dt", true);
        hudiOperatorService.operation(streamTableEnv,
                SQLOperator.builder().ddlSQLList(Lists.newArrayList(sourceDDL))
                        .coreSQLList(Lists.newArrayList("select * from " + sourceTableName)).build(),
                new Consumer<TableResult>() {
                    @Override
                    public void accept(TableResult tableResult) {
                        tableResult.print();
                    }
                });

        Thread.sleep(10000000);
    }

    @Test
    public void testBulkInsert() {
        StreamTableEnvironment streamTableEnv = FlinkEnvConfig.getStreamTableEnv();
        int cores = Runtime.getRuntime().availableProcessors();
        Map<String, Object> props = Maps.newHashMap();
        String sinkTableName = "bulk_insert_user";
        props.put(FlinkOptions.PATH.key(), "hdfs://hadoop:8020/user/flink/" + sinkTableName);
        props.put(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.COPY_ON_WRITE.name());
        props.put(FlinkOptions.PRECOMBINE_FIELD.key(), "update_time");
        props.put(FlinkOptions.RECORD_KEY_FIELD.key(), "id");
        props.put(FlinkOptions.PARTITION_PATH_FIELD.key(), "dt");
        props.put(FlinkOptions.TABLE_NAME.key(), sinkTableName);
        props.put(FlinkOptions.COMPACTION_ASYNC_ENABLED.key(), true);
        props.put(FlinkOptions.COMPACTION_DELTA_COMMITS.key(), 5);
        props.put(FlinkOptions.COMPACTION_SCHEDULE_ENABLED.key(), 5);
        props.put(FlinkOptions.COMPACTION_TASKS.key(), 20);
        props.put(FlinkOptions.COMPACTION_MAX_MEMORY.key(), 200);
        props.put(FlinkOptions.COMPACTION_TRIGGER_STRATEGY.key(), FlinkOptions.NUM_COMMITS);
        props.put(FlinkOptions.ARCHIVE_MAX_COMMITS.key(), 30);
        props.put(FlinkOptions.ARCHIVE_MIN_COMMITS.key(), 20);
        props.put(FlinkOptions.BUCKET_ASSIGN_TASKS.key(), cores);
        props.put(FlinkOptions.CLEAN_RETAIN_COMMITS.key(), 10);
        props.put(FlinkOptions.WRITE_TASKS.key(), cores);
        props.put(FlinkOptions.WRITE_BATCH_SIZE.key(), "128D");
        props.put(FlinkOptions.OPERATION.key(), WriteOperationType.BULK_INSERT.value());
        String sinkDDL = SqlBuilderFactory.getSqlBuilder(SQLEngine.FLINK, props, sinkTableName,
                Lists.newArrayList(ColumnInfo.builder()
                                .columnName("id")
                                .columnType("int").build(),
                        ColumnInfo.builder()
                                .columnName("age")
                                .columnType("int").build(),
                        ColumnInfo.builder()
                                .columnType("string")
                                .columnName("name").build(),
                        ColumnInfo.builder()
                                .columnName("create_time")
                                .columnType("date").build(),
                        ColumnInfo.builder()
                                .columnName("update_time")
                                .columnType("date").build(),
                        ColumnInfo.builder()
                                .columnName("dt")
                                .columnType("string").build())).generatorDDL();

        System.out.println(sinkDDL);
//        String sinkSQLDDL = HudiSqlConfig.getDDL(cores, sinkTableName, columns, "id",
//                "update_time", "dt", false);
//        String insertSQLDML = HudiSqlConfig.getDML(OperatorEnums.INSERT, "*", sinkTableName, sourceTableName, "");
//        SQLOperator sqlOperator = SQLOperator.builder()
//                .ddlSQLList(Lists.newArrayList(sourceSQLDDL, sinkSQLDDL))
//                .coreSQLList(Lists.newArrayList(insertSQLDML))
//                .build();
//        hudiOperatorService.operation(streamTableEnv, sqlOperator, new Consumer<TableResult>() {
//            @Override
//            public void accept(TableResult tableResult) {
//                tableResult.print();
//            }
//        });
    }

}
