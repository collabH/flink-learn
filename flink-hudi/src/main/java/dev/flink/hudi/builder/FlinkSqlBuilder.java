package dev.flink.hudi.builder;

import dev.flink.hudi.builder.column.ColumnInfo;

import java.util.List;
import java.util.Map;

/**
 * @fileName: FlinkSqlBuilder.java
 * @description: FlinkSqlBuilder.java类说明
 * @author: huangshimin
 * @date: 2021/12/8 8:21 下午
 */
public class FlinkSqlBuilder extends BaseSqlBuilder {
    public FlinkSqlBuilder(Map<String, Object> props, String tableName, List<ColumnInfo> columnInfos) {
        super(props, tableName, columnInfos);
        super.valid();
    }

    @Override
    public String generatorDDL() {
        StringBuilder sqlBuilder = new StringBuilder("CREATE TABLE ");
        sqlBuilder.append(super.getTableName()).append(" (");
        List<ColumnInfo> columnInfos = super.getColumnInfos();
        for (ColumnInfo columnInfo : columnInfos) {
            sqlBuilder.append(columnInfo.getColumnName()).append(" ")
                    .append(columnInfo.getColumnType()).append(",");
        }
        sqlBuilder.deleteCharAt(sqlBuilder.length() - 1);
        sqlBuilder.append(")with(");
        for (Map.Entry<String, Object> entry : super.getProps().entrySet()) {
            String configKey = entry.getKey();
            Object configValue = entry.getValue();
            sqlBuilder.append("'").append(configKey).append("'='")
                    .append(configValue)
                    .append("',");
        }
        sqlBuilder.deleteCharAt(sqlBuilder.length() - 1);
        sqlBuilder.append(")");
        return sqlBuilder.toString();
    }

    @Override
    public String generatorDML() {
        return null;
    }
}
