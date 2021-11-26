package dev.hudi

import dev.flink.hudi.constants.OperatorEnums
import org.apache.commons.lang3.StringUtils
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.configuration.FlinkOptions

/**
 * @fileName: HudiSqlConfig.scala
 * @description: hudiSql配置
 * @author: huangshimin
 * @date: 2021/11/19 3:18 下午
 */
object HudiSqlConfig {

  /**
   * 获取DDL
   *
   * @param parallelism
   * @param tableName
   * @param columns
   * @param preCombineKey
   * @param partitionKey
   * @return
   */
  def getDDL(parallelism: Int, tableName: String, columns: String, recordKey: String, preCombineKey: String,
             partitionKey: String, streamingRead: Boolean): String = {
    /**
     * 基础hudi配置
     */
    val baseHudiOptions =
      s"""  '${FlinkOptions.PATH.key()}' = 'hdfs://hadoop:8020/user/flink/${tableName}',
         |  '${FlinkOptions.TABLE_TYPE.key()}' = '${HoodieTableType.MERGE_ON_READ}',
         |   '${FlinkOptions.PRECOMBINE_FIELD.key}'='${preCombineKey}',
         |  '${FlinkOptions.RECORD_KEY_FIELD.key}'='${recordKey}',
         |  '${FlinkOptions.PARTITION_PATH_FIELD.key}'='${partitionKey}',
         |   '${FlinkOptions.TABLE_NAME.key}'='${tableName}',"""
    val compactionHudiOptions =
      s"""  '${FlinkOptions.COMPACTION_ASYNC_ENABLED.key}'='true',
         |  '${FlinkOptions.COMPACTION_DELTA_COMMITS.key}'='5',
         |  '${FlinkOptions.COMPACTION_SCHEDULE_ENABLED.key}'='true',
         |  '${FlinkOptions.COMPACTION_TASKS.key}'='20',
         |  '${FlinkOptions.COMPACTION_MAX_MEMORY.key}'='200',
         |  '${FlinkOptions.COMPACTION_TRIGGER_STRATEGY.key}'='${FlinkOptions.NUM_COMMITS}',"""
    val createTableConfig =
      s"""CREATE TABLE ${tableName}(
         | ${columns}
         |)
         |WITH (
         |  'connector' = 'hudi',"""

    val streamReadHudiOptuons: String =
      s"""
         |'${FlinkOptions.READ_AS_STREAMING.key()}'='true',
         |'${FlinkOptions.READ_STREAMING_CHECK_INTERVAL.key()}'='10',
         |'${FlinkOptions.READ_TASKS.key()}'='${parallelism}'
         |""".stripMargin

    // 开启流式读取ddl
    if (streamingRead) {
      val streamReadSQL: String =
        s"""
           |${createTableConfig}
           |${baseHudiOptions}
           |${streamReadHudiOptuons}
           |)
           |""".stripMargin
      streamReadSQL
    } else {
      // 正常ddl
      s"""
         |   ${createTableConfig}
         |   ${baseHudiOptions}
         |  '${FlinkOptions.ARCHIVE_MAX_COMMITS.key}'='30',
         |  '${FlinkOptions.ARCHIVE_MIN_COMMITS.key}'='20',
         |  '${FlinkOptions.BUCKET_ASSIGN_TASKS.key}'='${parallelism}',
         |  '${FlinkOptions.CLEAN_ASYNC_ENABLED.key}'='false',
         |  '${FlinkOptions.CLEAN_RETAIN_COMMITS.key}'='10',
         |   ${compactionHudiOptions}
         |  '${FlinkOptions.WRITE_TASKS.key}'='${parallelism}',
         |  '${FlinkOptions.WRITE_BATCH_SIZE.key}'='128D',
         |  '${FlinkOptions.OPERATION.key}'='upsert'
         |)
         |""".stripMargin
    }
  }

  def getDML(operation: OperatorEnums, columnNames: String, sinkTableName: String, sourceTableName: String,
             values: String): String = {
    operation match {
      case OperatorEnums.INSERT =>
        if (StringUtils.isNotEmpty(sourceTableName)) {
          s"""INSERT INTO $sinkTableName SELECT $columnNames FROM $sourceTableName"""
        } else {
          s"""INSERT INTO $sinkTableName $values"""
        }
      case OperatorEnums.DELETE => {
        ""
      }
      case OperatorEnums.QUERY => {
        ""
      }
    }
  }

  def getGeneratorSourceSQLDDL(tableName: String, columns: String) = {
    s"""
       |create table ${tableName}(
       |${columns}
       |)with(
       | 'connector'='datagen',
       | 'rows-per-second'='1',
       | 'fields.id.kind'='sequence',
       | 'fields.id.start'='1',
       | 'fields.id.end'='200',
       | 'fields.dt.kind'='sequence',
       | 'fields.dt.start'='202101',
       | 'fields.dt.end'='202112'
       |)
       |""".stripMargin
  }
}

