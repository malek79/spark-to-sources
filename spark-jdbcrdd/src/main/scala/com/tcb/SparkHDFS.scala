package com.tcb

import org.apache.log4j.LogManager
import org.apache.spark.sql.{ SaveMode, SparkSession }
object SparkHDFS {

  case class HelloWorld(message: String)

  def main(args: Array[String]): Unit = {

    val log = LogManager.getRootLogger
    // Creation of Spark Session
    val sparkSession = SparkSession.builder()
      .appName("example-spark-scala-read-and-write-from-hdfs")
      .master("local[*]")
    //  .config("HADOOP_USER_NAME", "hdfs")
      .getOrCreate()

    import sparkSession.implicits._

    // ====== Creating a dataframe with 1 partition
    val df = Seq(HelloWorld("helloworld")).toDF().coalesce(1)
    // System.setProperty("HADOOP_USER_NAME", "hdfs")
    // ======= Writing files
    // Writing file as parquet
    df.write.mode(SaveMode.Overwrite).parquet("/tmp/testwiki2")
    //  Writing file as csv
    df.write.mode(SaveMode.Overwrite).csv("/tmp/testwiki2.csv")
    // ======= Reading files
    // Reading parquet files
    val df_parquet = sparkSession.read.parquet("/tmp/testwiki")
    log.info(df_parquet.show())
    //  Reading csv files
    val df_csv = sparkSession.read.option("inferSchema", "true").csv("/tmp/testwiki.csv")
    log.info(df_csv.show())
  }
}