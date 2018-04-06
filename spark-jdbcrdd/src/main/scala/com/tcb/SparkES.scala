package com.tcb
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.elasticsearch.spark._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.serializer.KryoSerializer

object SparkES {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("name").setMaster("local[*]");
    conf.set("es.index.auto.create", "true");
 //   conf.set("spark.serializer", classOf[KryoSerializer].getName)
    val sc = new SparkContext(conf)

    val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
    val airports = Map("arrival" -> "Otopeni", "SFO" -> "San Fran")

    // sc.makeRDD(Seq(numbers, airports)).saveToEs("spark/docs")
    import org.elasticsearch.spark.rdd.EsSpark

    // define a case class
    case class Trip(departure: String, arrival: String)

    val upcomingTrip = Trip("OTP", "SFO")
    val lastWeekTrip = Trip("MUC", "OTP")

    val rdd = sc.makeRDD(Seq(upcomingTrip, lastWeekTrip))
    // EsSpark.saveToEs(rdd, "spark/docs")

    val RDD = sc.esRDD("spark/docs").collect()
    println("*************")
    RDD.map(println)
    println("*************")

    val RDDi = sc.esRDD("my_index/my_type").collect()
    println("*************")
    RDDi.map(println)
    println("*************")
    val sq = new SQLContext(sc)
    val df = sq.read.format("org.elasticsearch.spark.sql").load("spark/docs")

    df.printSchema()
    // root
    //|-- departure: string (nullable = true)
    //|-- arrival: string (nullable = true)
    //|-- days: long (nullable = true)

    val filter = df.filter(df("arrival").equalTo("OTP"))
    filter.show()
    println("*************")

  }
}