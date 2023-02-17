package dev.learn.flink.sqlconnector;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import java.math.BigInteger;
import java.util.List;

/**
 * @fileName: StringTransRowDataFunction.java
 * @description: StringTransRowDataFunction.java类说明
 * @author: huangshimin
 * @date: 2023/2/17 19:10
 */
public class StringTransRowDataFunction implements MapFunction<String, RowData> {
    private final ColumnHolder schema;

    public StringTransRowDataFunction(ColumnHolder schema) {
        this.schema = schema;
    }

    @Override
    public RowData map(String value) throws Exception {
        String[] arrs = value.split(",");
        List<Column> columns = schema.getColumns();
        GenericRowData genericRowData = new GenericRowData(RowKind.INSERT, columns.size());
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            switch (column.getDataType().getLogicalType().getTypeRoot()) {
                case VARCHAR:
                    genericRowData.setField(i, arrs[i]);
                    break;
                case INTEGER:
                    genericRowData.setField(i, Integer.valueOf(arrs[i]));
                    break;
                case BIGINT:
                    genericRowData.setField(i, Long.valueOf(arrs[i]));
                    break;
                default:
                    throw new RuntimeException("暂时不支持!");
            }
        }
        return genericRowData;
    }
}
