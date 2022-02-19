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
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.table.HoodieTableFactory;

import java.util.Map;
import java.util.function.Consumer;

/**
 * @fileName: AlluxioOnHudiDriver.java
 * @description: alluxio整合flink&hudi
 * @author: huangshimin
 * @date: 2022/2/18 10:26 下午
 */
public class AlluxioOnHudiDriver {
    public static void main(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String hudiSinkPath = parameterTool.get("hudiSinkPath", "alluxio://hadoop:19998/user/flink" +
                "/alluxio_hudi_user_t");
        String sinkTableName = parameterTool.get("hudiTableName", "hudi_user");
        HudiOperatorService<StreamTableEnvironment, SQLOperator,
                Consumer<TableResult>> streamHudiOperatorService = new SQLHudiOperatorService<>();
        StreamTableEnvironment streamTableEnv = FlinkEnvConfig.getStreamTableEnv();
        String sourceTableName = "source";
        // 通过flink datagen connector生成指定mock数据，写入hudi
        String sourceSQLDDL = "create table " + sourceTableName + " (id int," +
                "age int," +
                "name string," +
                "dt string)with('connector'='kafka','topic' = " +
                "'hudi_user_topic'," +
                "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "  'properties.group.id' = 'hudiUserGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json')";
        Map<String, Object> props = Maps.newHashMap();
        // hudi连接器配置
        props.put(FactoryUtil.CONNECTOR.key(), HoodieTableFactory.FACTORY_ID);
        // 设置hudi表写入path
        props.put(FlinkOptions.PATH.key(), hudiSinkPath);
        // 设置表类型
        props.put(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.COPY_ON_WRITE.name());
        // precombine字段设置，如果出现相同partition path&record key根据precombine字段的大小去最大的值
        props.put(FlinkOptions.PRECOMBINE_FIELD.key(), "dt");
        // record key，每个partition path下的唯一标识字段
        props.put(FlinkOptions.RECORD_KEY_FIELD.key(), "id");
        // 分区字段
        props.put(FlinkOptions.PARTITION_PATH_FIELD.key(), "dt");
        // hudi表名
        props.put(FlinkOptions.TABLE_NAME.key(), sinkTableName);
        // 最大和最小commit归档限制
        props.put(FlinkOptions.ARCHIVE_MAX_COMMITS.key(), 30);
        props.put(FlinkOptions.ARCHIVE_MIN_COMMITS.key(), 20);
        // bucket assign的任务书
        props.put(FlinkOptions.BUCKET_ASSIGN_TASKS.key(), 4);
        // 保留的commit数
        props.put(FlinkOptions.CLEAN_RETAIN_COMMITS.key(), 10);
        // 写入hudi的并行度
        props.put(FlinkOptions.WRITE_TASKS.key(), 4);
        // 写入一批的大小
        props.put(FlinkOptions.WRITE_BATCH_SIZE.key(), "128D");
        // hudi操作类型
        props.put(FlinkOptions.OPERATION.key(), WriteOperationType.UPSERT.value());
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
        String insertSQLDML = HudiSqlConfig.getDML(OperatorEnums.INSERT, "*", sinkTableName, sourceTableName, "");
        SQLOperator sqlOperator = SQLOperator.builder()
                .ddlSQLList(Lists.newArrayList(sinkDDL, sourceSQLDDL))
                .insertSQLList(Lists.newArrayList(insertSQLDML))
                .build();
        streamHudiOperatorService.operation(streamTableEnv, sqlOperator, new Consumer<TableResult>() {
            @Override
            public void accept(TableResult tableResult) {
                tableResult.print();
            }
        });
    }
}
