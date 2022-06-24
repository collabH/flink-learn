package dev.flink.hudi.driver;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import dev.flink.hudi.builder.SqlBuilderFactory;
import dev.flink.hudi.builder.column.ColumnInfo;
import dev.flink.hudi.config.FlinkEnvConfig;
import dev.flink.hudi.constants.SQLEngine;
import dev.flink.hudi.service.HudiOperatorService;
import dev.flink.hudi.service.sql.SQLHudiOperatorService;
import dev.flink.hudi.service.sql.SQLOperator;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.table.HoodieTableFactory;

import java.util.Map;
import java.util.function.Consumer;

/**
 * @fileName: CommonQueryDriver.java
 * @description: 统一查询JOB
 * @author: huangshimin
 * @date: 2021/12/16 8:57 下午
 */
public class CommonQueryDriver {
    public static void main(String[] args) {
        HudiOperatorService<StreamTableEnvironment, SQLOperator,
                Consumer<TableResult>> streamHudiOperatorService = new SQLHudiOperatorService<>();
        StreamTableEnvironment streamTableEnv = FlinkEnvConfig.getStreamTableEnv();
        String sourceTableName = "update_user_mor";
        Map<String, Object> props = Maps.newHashMap();
        props.put(FactoryUtil.CONNECTOR.key(), HoodieTableFactory.FACTORY_ID);
        props.put(FlinkOptions.PATH.key(), "file:///Users/huangshimin/Documents/study/hudi/storage/" + sourceTableName);
        props.put(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.COPY_ON_WRITE.name());
        props.put(FlinkOptions.PRECOMBINE_FIELD.key(), "dt");
        props.put(FlinkOptions.RECORD_KEY_FIELD.key(), "id");
        props.put(FlinkOptions.PARTITION_PATH_FIELD.key(), "dt");
        props.put(FlinkOptions.TABLE_NAME.key(), sourceTableName);
        props.put(FlinkOptions.READ_AS_STREAMING.key(), "true");
        props.put(FlinkOptions.READ_START_COMMIT.key(), "20211215000000");
        String sourceDDL = SqlBuilderFactory.getSqlBuilder(SQLEngine.FLINK, props, sourceTableName,
                Lists.newArrayList(
                        ColumnInfo.builder()
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
        streamHudiOperatorService.operation(streamTableEnv, SQLOperator.builder()
                        .querySQLList(Lists.newArrayList("select * from " + sourceTableName))
                        .ddlSQLList(Lists.newArrayList(sourceDDL)).build(),
                new Consumer<TableResult>() {
                    @Override
                    public void accept(TableResult tableResult) {
                        tableResult.print();
                    }
                });
    }
}
