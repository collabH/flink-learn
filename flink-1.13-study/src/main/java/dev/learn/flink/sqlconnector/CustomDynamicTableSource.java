package dev.learn.flink.sqlconnector;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
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
    private final DataFormat dataFormat;
    private final String path;

    public CustomDynamicTableSource(ResolvedSchema schema, DataFormat dataFormat, String path) {
        this.schema = schema;
        this.dataFormat = dataFormat;
        this.path = path;
    }

    @Override
    public DynamicTableSource copy() {
        return new CustomDynamicTableSource(this.schema, this.dataFormat, this.path);
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
        return new DataStreamScanProvider() {
            @Override
            public DataStream<RowData> produceDataStream(ProviderContext providerContext,
                                                         StreamExecutionEnvironment execEnv) {
                DataStreamSource<RowData> dataStreamSource;
                List<Column> columns = schema.getColumns();
                if (CollectionUtils.isEmpty(columns)) {
                    throw new RuntimeException("列配置异常");
                }
                switch (dataFormat) {
                    case CSV:
                        CsvSchema.Builder csvBuilder = CsvSchema.builder();
                        for (Column column : columns) {
                            csvBuilder.addColumn(column.getName(), transCsvType(column.getDataType()));
                        }
                        CsvReaderFormat<RowData> csvReaderFormat =
                                CsvReaderFormat.forSchema(csvBuilder.build(), TypeInformation.of(RowData.class));

                        FileSource<RowData> source = FileSource.forRecordStreamFormat(csvReaderFormat,
                                Path.fromLocalFile(FileUtils.getFile(path))).build();
                        return execEnv.fromSource(source,
                                WatermarkStrategy.noWatermarks(),
                                "custom-source");
                    case JSON:
                    default:
                        throw new RuntimeException("data format非法!");
                }
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
