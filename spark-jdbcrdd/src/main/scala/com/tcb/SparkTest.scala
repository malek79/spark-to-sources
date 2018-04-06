package com.tcb

import org.apache.spark.sql.SparkSession

object SparkTest {

  def main(args: Array[String]): Unit = {
    val ss: SparkSession = SparkSession.builder().appName("sparkTest").master("local[*]")
      .getOrCreate()
    val rdd = ss.sparkContext.parallelize(0 to 9) // [0,1,2,3,4,5,6,7,8,9]
    val rdd1 = rdd.repartition(10)
    println(rdd1.getNumPartitions)
    println(rdd.map(_ * 2).sum) //90
  }
}