package dev.learn.flink.tablesql.httpConnector.deserializer;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Charsets;
import org.apache.commons.lang3.ClassUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Objects;

/**
 * @fileName: HttpJsonBeanDeserializer.java
 * @description: HttpJsonBeanDeserializer.java类说明
 * @author: by echo huang
 * @date: 2021/1/17 7:16 下午
 */
public class HttpJsonBeanDeserializer implements DeserializationSchema<RowData> {
    private final List<LogicalType> parsingTypes;
    private final DynamicTableSource.DataStructureConverter converter;
    private final TypeInformation<RowData> producedTypeInfo;
    private final Boolean isArray;
    private final String formatClassName;

    public HttpJsonBeanDeserializer(List<LogicalType> parsingTypes, DynamicTableSource.DataStructureConverter converter, TypeInformation<RowData> producedTypeInfo, String formatClassNam, Boolean isArray) {
        this.parsingTypes = parsingTypes;
        this.formatClassName = formatClassNam;
        this.converter = converter;
        this.producedTypeInfo = producedTypeInfo;
        this.isArray = isArray;
    }

    @Override
    public RowData deserialize(byte[] bytes) throws IOException {
        String dataStr = new String(bytes, Charsets.UTF_8);
        Row row = new Row(RowKind.INSERT, parsingTypes.size());
        try {
            Class<?> formatClazz = ClassUtils.getClass(this.formatClassName);
            if (this.isArray) {
                List<?> arrayData = JSON.parseArray(dataStr, formatClazz);
                for (Object subData : arrayData) {
                    extracted(subData, row);
                }
            } else {
                Object data = JSON.parseObject(dataStr, formatClazz);
                extracted(data, row);
            }
        } catch (ClassNotFoundException | IllegalAccessException e) {
            throw new RuntimeException(formatClassName + "类不能存在!", e);
        }
        return (RowData) converter.toInternal(row);
    }

    private void extracted(Object data, Row row) throws IllegalAccessException {
        Class<?> subClazz = data.getClass();
        Field[] fields = subClazz.getFields();
        for (int i = 0; i < this.parsingTypes.size(); i++) {
            Field field = fields[i];
            field.setAccessible(true);
            row.setField(i, parse(this.parsingTypes.get(i).getTypeRoot(), String.valueOf(field.get(i))));
        }
    }

    private Object parse(LogicalTypeRoot root, String value) {
        switch (root) {
            case INTEGER:
                return Integer.parseInt(value);
            case VARCHAR:
                return value;
            case BOOLEAN:
                return Boolean.parseBoolean(value);
            case BIGINT:
                return Long.parseLong(value);
            default:
                throw new IllegalArgumentException();
        }
    }

    @Override
    public boolean isEndOfStream(RowData rowData) {
        return Objects.isNull(rowData);
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedTypeInfo;
    }
}
