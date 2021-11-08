package dev.learn.spark.hudi.config

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig

/**
 * @fileName: HudiConfig.scala
 * @description: Hudi配置
 * @author: huangshimin
 * @date: 2021/11/7 10:50 下午
 */
object HudiConfig {

  def getUserOptions() = Map(
    PRECOMBINE_FIELD.key() -> "updateTime",
    RECORDKEY_FIELD.key() -> "id",
    PARTITIONPATH_FIELD.key() -> "partition",
    HoodieWriteConfig.TBL_NAME.key() -> "user")
}
