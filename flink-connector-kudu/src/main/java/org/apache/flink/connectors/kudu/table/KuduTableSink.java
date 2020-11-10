/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connectors.kudu.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connectors.kudu.connector.KuduTableInfo;
import org.apache.flink.connectors.kudu.connector.writer.KuduWriterConfig;
import org.apache.flink.connectors.kudu.streaming.KuduSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.types.Row;

/**
 * 实现UpsertStreamTableSink upsert流
 */
public class KuduTableSink implements UpsertStreamTableSink<Row> {

    // 写入配置构造器
    private final KuduWriterConfig.Builder writerConfigBuilder;
    // TableSchema
    private final TableSchema flinkSchema;
    // kudu table配置包含schema、tableName、表参数
    private final KuduTableInfo tableInfo;

    public KuduTableSink(KuduWriterConfig.Builder configBuilder, KuduTableInfo tableInfo, TableSchema flinkSchema) {
        this.writerConfigBuilder = configBuilder;
        this.tableInfo = tableInfo;
        this.flinkSchema = flinkSchema;
    }

    @Override
    public void setKeyFields(String[] keyFields) { /* this has no effect */}

    @Override
    public void setIsAppendOnly(Boolean isAppendOnly) { /* this has no effect */}

    /**
     * 获取记录类型
     * @return
     */
    @Override
    public TypeInformation<Row> getRecordType() { return flinkSchema.toRowType(); }

    /**
     * 消费数据流
     * @param dataStreamTuple
     * @return
     */
    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStreamTuple) {
        // 创建kuduSink
        KuduSink upsertKuduSink = new KuduSink(writerConfigBuilder.build(), tableInfo, new UpsertOperationMapper(getTableSchema().getFieldNames()));

        // 调用底层DataStream
        return dataStreamTuple
                .addSink(upsertKuduSink)
                .setParallelism(dataStreamTuple.getParallelism())
                .name(TableConnectorUtils.generateRuntimeName(this.getClass(), getTableSchema().getFieldNames()));
    }

    @Override
    public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        return new KuduTableSink(writerConfigBuilder, tableInfo, flinkSchema);
    }

    @Override
    public TableSchema getTableSchema() { return flinkSchema; }
}
