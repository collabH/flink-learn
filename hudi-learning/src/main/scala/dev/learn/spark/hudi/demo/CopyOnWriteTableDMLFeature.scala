package dev.learn.spark.hudi.demo

import dev.learn.spark.hudi.context.RunContext
import org.apache.spark.sql.SparkSession

/**
 * @fileName: CopyOnWriteTableDMLFeature.scala
 * @description: cow表 dml操作
 * @author: huangshimin
 * @date: 2021/11/4 9:02 下午
 */
object CopyOnWriteTableDMLFeature {

  private val spark: SparkSession = RunContext.getHudiSpark()
  def main(args: Array[String]): Unit = {

  }
}
