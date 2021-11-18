package dev.learn.spark.hudi.config

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.callback.impl.HoodieWriteCommitHttpCallback
import org.apache.hudi.common.bloom.BloomFilterTypeCode
import org.apache.hudi.common.model.HoodieCleaningPolicy
import org.apache.hudi.common.table.marker.MarkerType
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieIndexConfig, HoodieWriteCommitCallbackConfig, HoodieWriteConfig}
import org.apache.hudi.keygen.SimpleKeyGenerator
import org.apache.hudi.table.action.compact.CompactionTriggerStrategy

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
   **/
    HoodieWriteConfig.MARKERS_TYPE.key() -> MarkerType.DIRECT.toString,
    HoodieWriteConfig.MARKERS_DELETE_PARALLELISM_VALUE -> "100",
    HoodieWriteConfig.MARKERS_TIMELINE_SERVER_BASED_BATCH_INTERVAL_MS.key() -> "50L",
    HoodieWriteConfig.MARKERS_TIMELINE_SERVER_BASED_BATCH_NUM_THREADS.key() -> "20",

    /**
     * compact相关配置
     */
    ASYNC_COMPACT_ENABLE.key() -> "true",

    /**
     * cleaner配置
     *
     */
    HoodieCompactionConfig.CLEANER_POLICY.key() -> HoodieCleaningPolicy.KEEP_LATEST_COMMITS,
    HoodieCompactionConfig.CLEANER_COMMITS_RETAINED.key() -> "2",
    HoodieCompactionConfig.ASYNC_CLEAN.key() -> "true",
    HoodieCompactionConfig.CLEANER_BOOTSTRAP_BASE_FILE_ENABLE.key() -> "true",
    HoodieCompactionConfig.CLEANER_FILE_VERSIONS_RETAINED.key() -> "3",
    HoodieCompactionConfig.CLEANER_INCREMENTAL_MODE_ENABLE.key() -> "true",
    HoodieCompactionConfig.CLEANER_PARALLELISM_VALUE.key() -> "100",

    /**
     * compact策略
     */
    HoodieCompactionConfig.INLINE_COMPACT.key() -> "false",
    // 提交多少次delta触发compact
    HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key() -> "5",
    // 提交delta后多长时间触发compact
    HoodieCompactionConfig.INLINE_COMPACT_TIME_DELTA_SECONDS.key() -> "60*60",
    // compact策略
    HoodieCompactionConfig.INLINE_COMPACT_TRIGGER_STRATEGY.key() -> CompactionTriggerStrategy.NUM_AND_TIME,

    /**
     * 元数据归档配置
     */
    HoodieCompactionConfig.MAX_COMMITS_TO_KEEP.key() -> "30",
    HoodieCompactionConfig.MIN_COMMITS_TO_KEEP.key() -> "20",
    HoodieCompactionConfig.COMMITS_ARCHIVAL_BATCH_SIZE.key() -> "10",

    /**
     * index配置
     */
    HoodieIndexConfig.INDEX_TYPE.key() -> "BLOOM",
    HoodieIndexConfig.BLOOM_FILTER_TYPE.key() -> BloomFilterTypeCode.DYNAMIC_V0.name(),

    /**
     * 事务提交回调
     */
    HoodieWriteCommitCallbackConfig.TURN_CALLBACK_ON.key() -> "true",
    HoodieWriteCommitCallbackConfig.CALLBACK_CLASS_NAME.key() -> classOf[HoodieWriteCommitHttpCallback].getName,
    HoodieWriteCommitCallbackConfig.CALLBACK_HTTP_URL.key() -> "http://helo.com",
    HoodieWriteCommitCallbackConfig.CALLBACK_HTTP_API_KEY_VALUE.key() -> "hudi_write_commit_http_callback",
    HoodieWriteCommitCallbackConfig.CALLBACK_HTTP_TIMEOUT_IN_SECONDS.key() -> "3",

    /**
     * hudi keygenerators配置
     */
    KEYGENERATOR_CLASS_NAME.key()->classOf[SimpleKeyGenerator].getName,
    RECORDKEY_FIELD.key()->"ts",
    PARTITIONPATH_FIELD.key() -> "partition",
    URL_ENCODE_PARTITIONING.key()->"true",
    HIVE_STYLE_PARTITIONING.key()->"true"
  )


  def getUserOptions() = Map(
    PRECOMBINE_FIELD.key() -> "updateTime",
    RECORDKEY_FIELD.key() -> "id",
    PARTITIONPATH_FIELD.key() -> "partition",
    HoodieWriteConfig.TBL_NAME.key() -> "user")

}
