package dev.learn.spark.hudi.data

import java.sql.Date

import org.apache.spark.sql.SparkSession

/**
 * @fileName: DataGenerator.scala
 * @description: 数据处理器
 * @author: huangshimin
 * @date: 2021/11/4 9:04 下午
 */
object DataGenerator {

  def getUserData(spark: SparkSession) = {
    val date = new Date(System.currentTimeMillis())
    val user1 = User(1, "hsm", 25, date, date)
    val user2 = User(2, "wy", 25, date, date)
    val user3 = User(3, "hello", 20, date, date)
    val user11 = User(1, "ls", 25, date, date)
    import spark.implicits._
    spark.sparkContext.makeRDD(Seq(user1, user2, user3, user11)).toDF()
  }

}

case class User(id: Long, name: String, age: Int, createTime: Date, updateTime: Date)