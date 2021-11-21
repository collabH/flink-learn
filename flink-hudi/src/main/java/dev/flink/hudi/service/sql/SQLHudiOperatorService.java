package dev.flink.hudi.service.sql;

import dev.flink.hudi.service.HudiOperatorService;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.Consumer;

/**
 * @fileName: FlinkSQHudiOperatorService.java
 * @description: flink sql操作hudi
 * @author: huangshimin
 * @date: 2021/11/18 5:13 下午
 */

public class SQLHudiOperatorService implements HudiOperatorService<StreamTableEnvironment, SQLOperator,
        Consumer<Row>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SQLHudiOperatorService.class);

    @Override
    public void operation(StreamTableEnvironment streamTableEnvironment, SQLOperator sqlOperator,
                          Consumer<Row> collector) {
        sqlOperator.checkParams();
        List<String> ddlSQLList = sqlOperator.getDdlSQLList();
        for (String ddlSQL : ddlSQLList) {
            LOGGER.info("execute DDL SQL:{}", ddlSQL);
            streamTableEnvironment.executeSql(ddlSQL);
        }
        List<String> coreSQLList = sqlOperator.getCoreSQLList();
        for (String coreSQL : coreSQLList) {
            LOGGER.info("execute core SQL:{}", coreSQL);
            TableResult tableResult = streamTableEnvironment.executeSql(coreSQL);
            tableResult.print();
            CloseableIterator<Row> collect = tableResult.collect();
            while (collect.hasNext()) {
                Row row = collect.next();
                collector.accept(row);
            }
        }
    }
}
