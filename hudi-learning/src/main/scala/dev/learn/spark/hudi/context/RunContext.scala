package dev.learn.spark.hudi.context

import org.apache.spark.sql.SparkSession

/**
 * @fileName: RunContext.scala
 * @description: spark运行环境
 * @author: huangshimin
 * @date: 2021/11/3 8:16 下午
 */
object RunContext {

  def getHudiSpark() = {
    SparkSession.builder().appName("hudi-spark")
      .master("local[*]")
      .getOrCreate()
  }
}
