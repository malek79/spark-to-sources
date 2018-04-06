package com.tcb

import org.apache.hadoop.yarn.webapp.example.HelloWorld
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object pmrc {

  def main(args: Array[String]): Unit = {

    // Creation of Spark Session
    val sparkSession = SparkSession.builder()
      .appName("pmrc")
      .master("local[*]")
      //  .config("HADOOP_USER_NAME", "hdfs")
      .getOrCreate()

    import sparkSession.implicits._

    val data = sparkSession.read.option("header", "true").option("delimiter", ';').csv("file:///home/malek/Downloads/pmrc3.csv")

    data.printSchema()
    println(data.count())
    data.select("ADRESSE").show()
    print(data.toJavaRDD.toDebugString())
    println(data.na.drop(Array("N_REGISTRE")).count())
  }
}