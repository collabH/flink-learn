package dev.learn.spark.hudi.demo

import dev.learn.spark.hudi.config.HudiConfig
import dev.learn.spark.hudi.context.RunContext
import dev.learn.spark.hudi.data.DataGenerator
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.QuickstartUtils
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * @fileName: CopyOnWriteTableDMLFeature.scala
 * @description: cow表 dml操作
 * @author: huangshimin
 * @date: 2021/11/4 9:02 下午
 */
object CopyOnWriteTableDMLFeature {

  private val spark: SparkSession = RunContext.getHudiSpark()
  private val USER_HDFS_PATH = "hdfs://localhost:8082/user/hudi/warehouse/user"

  def main(args: Array[String]): Unit = {
    //    writeData
    //    readData
    //        updateData
    //    readData
    //    writeData
    //    incrementalQuery

    //    rangeQuery

    // delete
    //    readData
    //    deleteData
    //    readData

    // insert overWrite table

    //    readData
    //    insertOverWriteTable
    //    readData

    // insert overwrite partition
    //    writeData
    readData
    insertOverWriteTable
    readData
  }

  /**
   * 覆盖partition，类似于hive的动态分区overwrite
   */
  def insertOverWriteParttition = {
    DataGenerator.getOverPartition(spark)
      .write.format("org.apache.hudi")
      .options(QuickstartUtils.getQuickstartWriteConfigs)
      .option(OPERATION.key(), INSERT_OVERWRITE_OPERATION_OPT_VAL)
      .options(HudiConfig.getUserOptions())
      .mode(Append)
      .save(USER_HDFS_PATH)
  }

  /**
   * 在Hudi元数据级别逻辑上覆盖表。Hudi清理器最终会清理上一个表快照的文件组。这比删除旧表并在覆盖模式下重新创建要快。
   * 覆盖整个表
   */
  def insertOverWriteTable = {
    DataGenerator.getDeleteData(spark)
      .write.format("org.apache.hudi")
      .options(QuickstartUtils.getQuickstartWriteConfigs)
      .option(OPERATION.key(), INSERT_OVERWRITE_TABLE_OPERATION_OPT_VAL)
      .options(HudiConfig.getUserOptions())
      .mode(Append)
      .save(USER_HDFS_PATH)
  }

  def deleteData = {
    DataGenerator.getDeleteData(spark)
      .write.format("org.apache.hudi")
      .options(QuickstartUtils.getQuickstartWriteConfigs)
      .option(OPERATION.key(), DELETE_OPERATION_OPT_VAL)
      .options(HudiConfig.getUserOptions())
      .mode(Append)
      .save(USER_HDFS_PATH)

  }

  def rangeQuery = {
    spark.read.format("org.apache.hudi")
      .option(QUERY_TYPE.key(), QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(BEGIN_INSTANTTIME.key(), "20211108164930")
      .option(END_INSTANTTIME.key(), "20211108165305")
      .load(USER_HDFS_PATH)
      .show()
  }

  def incrementalQuery = {
    spark.read.format("org.apache.hudi")
      .load(USER_HDFS_PATH)
      .createOrReplaceTempView("hudi_trips_snapshot")
    import spark.implicits._
    // 获取全部的commit时间
    val commits: Array[String] = spark.sql("select distinct(_hoodie_commit_time) as commitTime from  hudi_trips_snapshot order by commitTime")
      .map((k: Row) => k.getString(0)).take(50)
    // 获取最后的commitTs
    val beginTime: String = commits(commits.length - 1)

    // incrementally query data
    val tripsIncrementalDF: DataFrame = spark.read.format("hudi").
      option(QUERY_TYPE.key(), QUERY_TYPE_INCREMENTAL_OPT_VAL).
      option(BEGIN_INSTANTTIME.key(), beginTime).
      load(USER_HDFS_PATH)
    tripsIncrementalDF.createOrReplaceTempView("hudi_trips_incremental")

    spark.sql("select `_hoodie_commit_time`, id, name, age, createTime,updateTime,partition from  " +
      "hudi_trips_incremental where " +
      "id > 0").show()
  }

  /**
   * partition需要相同
   */
  def updateData = {
    DataGenerator.getUpdateUserData(spark).write.format("org.apache.hudi")
      .options(QuickstartUtils.getQuickstartWriteConfigs)
      .options(HudiConfig.getUserOptions())
      .option("hoodie.keep.max.commits", "3")
      .option("hoodie.keep.min.commits", "2")
      .option("hoodie.cleaner.commits.retained", "1")
      .option(TABLE_TYPE.key(), COW_TABLE_TYPE_OPT_VAL)
      .option(OPERATION.key(), UPSERT_OPERATION_OPT_VAL)
      .mode(Append)
      .save(USER_HDFS_PATH)
  }


  def readData: Unit = {
    spark.read.format("org.apache.hudi")
      .option(TABLE_TYPE.key(), COW_TABLE_TYPE_OPT_VAL)
      // 根据时间读取数据
      //      .option(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key(), "20211101154909")
      .load(USER_HDFS_PATH)
      .show(10)
  }

  /**
   * 通过spark写数据至hdfs
   */
  def writeData: Unit = {
    val df: DataFrame = DataGenerator.getUserData(spark)
    df.write.format("org.apache.hudi")
      .options(QuickstartUtils.getQuickstartWriteConfigs)
      .options(HudiConfig.getUserOptions())
      .option("hoodie.keep.max.commits", "3")
      .option("hoodie.keep.min.commits", "2")
      .option("hoodie.cleaner.commits.retained", "1")
      .option(TABLE_TYPE.key(), COW_TABLE_TYPE_OPT_VAL)
      .mode(Overwrite)
      .save(USER_HDFS_PATH)
  }
}
