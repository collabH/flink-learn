package dev.learn.flink.tablesql.httpConnector.deserializer;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

import java.util.List;

/**
 * @fileName: HttpJsonBeanFormat.java
 * @description: HttpJsonBeanFormat.java类说明
 * @author: by echo huang
 * @date: 2021/1/17 7:11 下午
 */
public class HttpJsonBeanFormat implements DecodingFormat<DeserializationSchema<RowData>> {
    private final String formatClassName;
    private final Boolean isArray;

    public HttpJsonBeanFormat(String formatClassName, Boolean isArray) {
        this.formatClassName = formatClassName;
        this.isArray = isArray;
    }

    @Override
    public DeserializationSchema<RowData> createRuntimeDecoder(DynamicTableSource.Context context, DataType producedDataType) {
        // create type information for the DeserializationSchema
        final TypeInformation<RowData> producedTypeInfo = context.createTypeInformation(
                producedDataType);

        // most of the code in DeserializationSchema will not work on internal data structures
        // create a converter for conversion at the end
        final DynamicTableSource.DataStructureConverter converter = context.createDataStructureConverter(producedDataType);

        // use logical types during runtime for parsing
        List<LogicalType> parsingTypes = producedDataType.getLogicalType().getChildren();

        // create runtime class
        return new HttpJsonBeanDeserializer(parsingTypes, converter, producedTypeInfo, formatClassName, isArray);
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .build();
    }
}
