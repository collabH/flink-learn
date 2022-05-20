package dev.learn.spark.hudi.demo

import org.apache.spark.Partitioner
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @fileName: SparkSqlDemo.scala
 * @description: spark sql demo
 * @author: huangshimin
 * @date: 2022/4/26 7:18 PM
 */
object SparkSqlDemo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder().appName("spark-sql-demo")
      .master("local")
      // 关闭自动broadcast优化
      //      .config("spark.sql.autoBroadcastJoinThreshold","10")
      .getOrCreate()
    import spark.implicits._

    val df1: DataFrame = Seq((1, "hello"), (2, "your"), (3, "my"), (3, "my"), (3, "my"), (1, "my"), (3, "my"), (3,
      "my"), (3, "my"), (3, "my"), (3, "my"), (1, "my")).toDF()
      .withColumnRenamed("_1", "id")
      .withColumnRenamed("_2", "name")
    val df2: DataFrame = Seq((1, "world"), (2, "dear"), (3, "bear")).toDF().withColumnRenamed("_1", "id")
      .withColumnRenamed("_2", "name1")
    df1.repartition($"test")
    // 关键依赖
    //    df1.join(df2.hint("SHUFFLE_HASH"),
    //      .hint("broadcast"),
    //      Seq("id")).show()
    df1.createTempView("df1")
    df2.createTempView("df2")
    // hint
    spark.sql("select /*+MAPJOIN(df2)*/ * from df1 right JOIN df2 ON df1.id=df2.id").show()
    //    spark.sql("select /*+SHUFFLE_MERGE(df1)*/ * from df1 INNER JOIN df2 ON df1.id=df2.id").show()

    spark.sparkContext.makeRDD(Seq("a", "b", "c")).map(data => (data, 1))
      .partitionBy(new CustomPartition(8))
      .foreachPartition(iter=>{
        for (elem <- iter) {
          elem._1 match {
            case "a" => // todo
            case "b" => // todo
          }
        }
      })

    // CBO
    //    spark.sql("ANALYZE TABLE df1 COMPUTE STATISTICS").show()
    //    spark.sql("ANALYZE TABLE df2 COMPUTE STATISTICS").show()
    // AQE:3.0.0之后特性，3.2.0默认开启，通过spark.sql.adaptive.enabled设置，默认优化合并shuffle write分区小文件、转换sort-merge join为broadcast
    // join，join倾斜优化。


    // 抽样key的分布
    //    df1.sample(true,0.9,5).show()




    Thread.sleep(1000000000);
  }

}

case class CustomPartition(partitions: Int) extends Partitioner {
  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = key match {
    case "a" => 0
    case "b" => 1
  }
}
