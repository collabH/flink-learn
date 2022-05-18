package dev.learn.spark.hudi.demo

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

/**
 * @fileName: SparkCoreDemo.scala
 * @description: SparkCoreDemo.scala类说明
 * @author: huangshimin
 * @date: 2022/4/24 2:41 PM
 */
object SparkCoreDemo {


  def main(args: Array[String]): Unit = {
    def conf: SparkConf = new SparkConf()
      .setAppName("spark-core-app")
      .setMaster("local")

    val sc: SparkContext = SparkContext.getOrCreate(conf)

    // combineByKey
    //    sc.makeRDD(Seq((1, "spark"), (2, "flink"), (3, "hudi"), (3, "iceberg")))
    //      .combineByKey((score: String) => (1, score),
    //        (c1: (Int, String), newScore: String) => (c1._1 + 1, c1._2 + newScore),
    //        (c1: (Int, String), c2: (Int, String)) => (c1._1 + c2._1, c1._2 + c2._2)
    //      ).foreach(println)

    // reduce
    //    println(sc.makeRDD(Seq(1, 2, 3, 4, 5, 6))
    //      .reduce((_1: Int, _2: Int) => _1 + _2))
    val spark: SparkSession = SparkSession.builder().master("local")
      .appName("core").getOrCreate()
    import spark.implicits._
    val value: Dataset[Int] = Seq(1, 2, 3, 4, 5).toDS()
    //    value.show(10)


    value.createOrReplaceGlobalTempView("txt")

    //    spark.sql("select * from global_temp.txt").show()

    //    println(value.columns.mkString("Array(", ", ", ")"))
    //    value.select(column("value"))

    //    value.sample(true,0.5,1).show()
    value.sort($"value".desc_nulls_first).show()

  }
}
