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
    val user1 = User(1, "hsm", 25, date, date, "20210901")
    val user2 = User(2, "wy", 25, date, date, "20210902")
    val user3 = User(3, "hello", 20, date, date, "20210903")
    val user11 = User(1, "ls", 25, date, date, "20210904")
    val user12 = User(2, "ls1", 25, date, date, "20210904")
    val user13 = User(3, "ls2", 25, date, date, "20210904")
    val user14 = User(4, "ls3", 25, date, date, "20210904")
    import spark.implicits._
    spark.sparkContext.makeRDD(Seq(user1, user12, user13, user14, user2, user3, user11)).toDF()
  }

  def getOverPartition(spark: SparkSession) = {
    val date = new Date(System.currentTimeMillis())
    val user2 = User(2, "hsm1", 25, date, date, "20210904")
    val user3 = User(3, "hsm2", 20, date, date, "20210904")
    import spark.implicits._
    spark.sparkContext.makeRDD(Seq(user2, user3)).toDF()
  }

  def getDeleteData(spark: SparkSession) = {
    val date = new Date(System.currentTimeMillis())
    val user2 = User(2, "hsm1", 25, date, date, "20210902")
    val user3 = User(3, "hsm2", 20, date, date, "20210903")
    import spark.implicits._
    spark.sparkContext.makeRDD(Seq(user2, user3)).toDF()
  }

  def getUpdateUserData(spark: SparkSession) = {
    val date = new Date(System.currentTimeMillis())
    val user2 = User(2, "hsm1", 25, date, date, "20210902")
    val user3 = User(3, "hsm2", 20, date, date, "20210903")
    val user11 = User(1, "zas", 24, date, date, "20210901")
    import spark.implicits._
    spark.sparkContext.makeRDD(Seq(user2, user3, user11)).toDF()
  }

}

case class User(id: Long, name: String, age: Int, createTime: Date, updateTime: Date,
                partition: String)