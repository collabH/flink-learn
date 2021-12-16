package dev.flink.hudi.service.sql;

import dev.flink.hudi.service.HudiOperatorService;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
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

public class SQLHudiOperatorService<ENV extends TableEnvironment> implements HudiOperatorService<ENV, SQLOperator,
        Consumer<TableResult>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SQLHudiOperatorService.class);

    @Override
    public void operation(ENV streamTableEnvironment, SQLOperator sqlOperator,
                          Consumer<TableResult> collector) {
        sqlOperator.checkParams();
        List<String> ddlSQLList = sqlOperator.getDdlSQLList();
        for (String ddlSQL : ddlSQLList) {
            LOGGER.info("execute DDL SQL:{}", ddlSQL);
            streamTableEnvironment.executeSql(ddlSQL);
        }
        List<String> querySQLList = sqlOperator.getQuerySQLList();
        if (null != querySQLList) {
            for (String querySql : querySQLList) {
                LOGGER.info("execute query SQL:{}", querySql);
                collector.accept(streamTableEnvironment.executeSql(querySql));
            }
        }
        List<String> insertSQLList = sqlOperator.getInsertSQLList();
        StatementSet statementSet = streamTableEnvironment.createStatementSet();
        if (null != insertSQLList) {
            for (String coreSQL : insertSQLList) {
                LOGGER.info("execute insert SQL:{}", coreSQL);
                statementSet.addInsertSql(coreSQL);
                collector.accept(statementSet.execute());
            }
        }

    }
}
