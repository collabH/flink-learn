package dev.learn.spark.hudi.config

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.common.table.marker.MarkerType
import org.apache.hudi.config.HoodieWriteConfig

/**
 * @fileName: HudiConfig.scala
 * @description: Hudi配置
 * @author: huangshimin
 * @date: 2021/11/7 10:50 下午
 */
object HudiConfig {

  Map(/**
   * markers机制相关配置：
   * Hudi中的marker是一个表示存储中存在对应的数据文件的标签，Hudi使用它在故障和回滚场景中自动清理未提交的数据。
   *
   * */
    HoodieWriteConfig.MARKERS_TYPE.key() -> MarkerType.DIRECT.toString,
    HoodieWriteConfig.MARKERS_DELETE_PARALLELISM_VALUE -> "100",
    HoodieWriteConfig.MARKERS_TIMELINE_SERVER_BASED_BATCH_INTERVAL_MS.key() -> "50L",
    HoodieWriteConfig.MARKERS_TIMELINE_SERVER_BASED_BATCH_NUM_THREADS.key() -> "20",

    /**
     * compact相关配置
     */
    ASYNC_COMPACT_ENABLE.key() -> "true")

  def getUserOptions() = Map(
    PRECOMBINE_FIELD.key() -> "updateTime",
    RECORDKEY_FIELD.key() -> "id",
    PARTITIONPATH_FIELD.key() -> "partition",
    HoodieWriteConfig.TBL_NAME.key() -> "user")

}
