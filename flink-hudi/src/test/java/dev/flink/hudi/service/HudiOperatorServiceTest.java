package dev.flink.hudi.service;

import com.google.common.collect.Lists;
import dev.flink.hudi.config.FlinkEnvConfig;
import dev.flink.hudi.constants.OperatorEnums;
import dev.flink.hudi.service.sql.SQLHudiOperatorService;
import dev.flink.hudi.service.sql.SQLOperator;
import dev.hudi.HudiSqlConfig;
import org.apache.flink.table.api.TableResult;
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
            Consumer<TableResult>> hudiOperatorService = new SQLHudiOperatorService();


    @Test
    public void testFlinkSQLOnHudi() {

        StreamTableEnvironment streamTableEnv = FlinkEnvConfig.getStreamTableEnv();
        int cores = Runtime.getRuntime().availableProcessors();
        String columns = "id int,age int,name string,create_time date,update_time date,dt string";
        String sourceTableName="source";
        String sinkTableName="hudi_user";
        String sourceSQLDDL = HudiSqlConfig.getGeneratorSourceSQLDDL(sourceTableName,columns);
        String sinkSQLDDL = HudiSqlConfig.getDDL(cores, sinkTableName, columns, "id",
                "update_time", "dt");
        String insertSQLDML = HudiSqlConfig.getDML(OperatorEnums.INSERT, "*", sinkTableName, sourceTableName, "");
        SQLOperator sqlOperator = SQLOperator.builder()
                .ddlSQLList(Lists.newArrayList(sourceSQLDDL,sinkSQLDDL))
                .coreSQLList(Lists.newArrayList( insertSQLDML))
                .build();
        hudiOperatorService.operation(streamTableEnv, sqlOperator, new Consumer<TableResult>() {
            @Override
            public void accept(TableResult tableResult) {
                tableResult.print();
            }
        });
    }


}
