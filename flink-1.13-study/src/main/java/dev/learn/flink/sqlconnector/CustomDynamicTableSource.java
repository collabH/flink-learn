package dev.learn.flink.sqlconnector;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

import java.util.List;

/**
 * @fileName: CustomDynamicTableSource.java
 * @description: CustomDynamicTableSource.java类说明
 * @author: huangshimin
 * @date: 2023/2/13 19:46
 */
public class CustomDynamicTableSource implements ScanTableSource {
    private final ResolvedSchema schema;
    private final String path;
    private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;

    public CustomDynamicTableSource(DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
                                    ResolvedSchema schema, String path) {
        this.schema = schema;
        this.path = path;
        this.decodingFormat = decodingFormat;
    }

    @Override
    public DynamicTableSource copy() {
        return new CustomDynamicTableSource(decodingFormat, this.schema, this.path);
    }

    @Override
    public String asSummaryString() {
        return "custom table source";
    }

    @Override
    public ChangelogMode getChangelogMode() {
        // 仅支持insert
        return ChangelogMode.newBuilder().addContainedKind(RowKind.INSERT).build();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
        DeserializationSchema<RowData> deserializer = decodingFormat.createRuntimeDecoder(context,
                schema.toPhysicalRowDataType());
        ColumnHolder columnHolder = new ColumnHolder(schema.getColumns());
        return new DataStreamScanProvider() {
            @Override
            public DataStream<RowData> produceDataStream(ProviderContext providerContext,
                                                         StreamExecutionEnvironment execEnv) {
                FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineInputFormat(),
                        new Path(path)).build();
                return execEnv.fromSource(fileSource,
                                WatermarkStrategy.noWatermarks(),
                                "custom_source")
                        .map((MapFunction<String, RowData>)  value -> {
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
                        })
                        .name("custom_string_trans_rowdata")
                        .uid("custom_string_trans_rowdata_uid");
            }

            @Override
            public boolean isBounded() {
                return true;
            }
        };

    }

    private CsvSchema.ColumnType transCsvType(DataType dataType) {
        LogicalType logicalType = dataType.getLogicalType();
        switch (logicalType.getTypeRoot()) {
            case VARCHAR:
                return CsvSchema.ColumnType.STRING;
            case BIGINT:
                return CsvSchema.ColumnType.NUMBER;
            default:
                return CsvSchema.ColumnType.NUMBER_OR_STRING;
        }
    }
}
