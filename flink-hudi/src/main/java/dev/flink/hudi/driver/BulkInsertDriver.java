package dev.flink.hudi.driver;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import dev.flink.hudi.builder.SqlBuilderFactory;
import dev.flink.hudi.builder.column.ColumnInfo;
import dev.flink.hudi.config.FlinkEnvConfig;
import dev.flink.hudi.constants.OperatorEnums;
import dev.flink.hudi.constants.SQLEngine;
import dev.flink.hudi.service.HudiOperatorService;
import dev.flink.hudi.service.sql.SQLHudiOperatorService;
import dev.flink.hudi.service.sql.SQLOperator;
import dev.hudi.HudiSqlConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.table.HoodieTableFactory;

import java.util.Map;
import java.util.function.Consumer;

/**
 * @fileName: BulkInsertDriver.java
 * @description: bulk insert
 * @author: huangshimin
 * @date: 2021/12/16 7:28 下午
 */
public class BulkInsertDriver {
    public static void main(String[] args) {
        HudiOperatorService<TableEnvironment, SQLOperator,
                Consumer<TableResult>> batchHudiOperatorService = new SQLHudiOperatorService<>();
        TableEnvironment tableEnv = FlinkEnvConfig.getBatchTableEnv();
//        int cores = Runtime.getRuntime().availableProcessors();
        int cores = 1;

        Map<String, Object> props = Maps.newHashMap();
        String sinkTableName = "bulk_insert_user";
        props.put(FactoryUtil.CONNECTOR.key(), HoodieTableFactory.FACTORY_ID);
        props.put(FlinkOptions.PATH.key(), "hdfs://hadoop:8020/user/flink/" + sinkTableName);
        props.put(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.COPY_ON_WRITE.name());
        props.put(FlinkOptions.PRECOMBINE_FIELD.key(), "dt");
        props.put(FlinkOptions.RECORD_KEY_FIELD.key(), "id");
        props.put(FlinkOptions.PARTITION_PATH_FIELD.key(), "dt");
        props.put(FlinkOptions.TABLE_NAME.key(), sinkTableName);
        props.put(FlinkOptions.COMPACTION_ASYNC_ENABLED.key(), true);
        props.put(FlinkOptions.COMPACTION_DELTA_COMMITS.key(), 5);
        props.put(FlinkOptions.COMPACTION_SCHEDULE_ENABLED.key(), true);
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
                                .columnName("dt")
                                .columnType("string").build())).generatorDDL();
        String insertSQLDML = HudiSqlConfig.getDML(OperatorEnums.INSERT, "*", sinkTableName, "", "VALUES(1,24," +
                "'hsm','20211216')");
        String insertSQLDML1 = HudiSqlConfig.getDML(OperatorEnums.INSERT, "*", sinkTableName, "", "VALUES(2,24," +
                "'hsm','20211216')");
        String insertSQLDML2 = HudiSqlConfig.getDML(OperatorEnums.INSERT, "*", sinkTableName, "", "VALUES(3,24," +
                "'hsm','20211216')");
        SQLOperator sqlOperator = SQLOperator.builder()
                .ddlSQLList(Lists.newArrayList(sinkDDL))
                .insertSQLList(Lists.newArrayList(insertSQLDML, insertSQLDML1, insertSQLDML2))
                .build();
        batchHudiOperatorService.operation(tableEnv, sqlOperator, new Consumer<TableResult>() {
            @Override
            public void accept(TableResult tableResult) {
                tableResult.print();
            }
        });
    }
}
