package dev.flink.hudi.builder;

import com.google.common.base.Preconditions;
import dev.flink.hudi.builder.column.ColumnInfo;
import dev.flink.hudi.constants.SQLEngine;

import java.util.List;
import java.util.Map;

/**
 * @fileName: SqlBuilderFactory.java
 * @description: sql构造器工厂
 * @author: huangshimin
 * @date: 2021/12/8 7:55 下午
 */
public class SqlBuilderFactory {
    public static BaseSqlBuilder getSqlBuilder(SQLEngine sqlEngine,
                                               Map<String, Object> props, String tableName,
                                               List<ColumnInfo> columnInfos) {
        Preconditions.checkNotNull(sqlEngine);
        BaseSqlBuilder sqlBuilder;
        switch (sqlEngine) {
            case FLINK:
            case SPARK:
                return new FlinkSqlBuilder(props, tableName, columnInfos);
            default:
                throw new RuntimeException(String.format("该SQL:%s引擎暂时不支持!", sqlEngine.name()));

        }
    }
}
