package com.tcb

import org.apache.hadoop.conf.Configuration

//import org.apache.hadoop.hbase.util.HBaseConfTool

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.client.ConnectionFactory

import org.apache.hadoop.hbase.TableName

import org.apache.hadoop.hbase.util.Bytes

import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.jcp.xml.dsig.internal.dom.ApacheData
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.util.StatCounter

object tessst {

  val conf: Configuration = HBaseConfiguration.create()
  val ss: SparkSession = SparkSession.builder().appName("SparkSaveToDb").master("local[*]")
    .getOrCreate()
  def main(args: Array[String]): Unit = {

    val connection: Connection = ConnectionFactory.createConnection(conf)
    val admin = new HBaseAdmin(conf)
    val table = connection.getTable(TableName.valueOf("employee"))
    conf.set(TableInputFormat.INPUT_TABLE, "employee")
    print("connection created")

    for (rowKey <- 1 to 1) {

      val result = table.get(new Get(Bytes.toBytes(rowKey.toString())))

      val nameDetails = result.getValue(Bytes.toBytes("emp personal data"), Bytes.toBytes("name"))

      val cityDetails = result.getValue(Bytes.toBytes("emp personal data"), Bytes.toBytes("city"))

      val designationDetails = result.getValue(Bytes.toBytes("emp professional data"), Bytes.toBytes("designation"))

      val salaryDetails = result.getValue(Bytes.toBytes("emp professional data"), Bytes.toBytes("salary"))

      val name = Bytes.toString(nameDetails)

      val city = Bytes.toString(cityDetails)

      val designation = Bytes.toString(designationDetails)

      val salary = Bytes.toString(salaryDetails)

      println("Name is " + name + ", city " + city + ", Designation " + designation + ", Salary " + salary)

    }
    // Read from Hbase using newAPIHadoopRDD
    val HBASERDD = ss.sparkContext.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable], classOf[org.apache.hadoop.hbase.client.Result])

    println(HBASERDD.count())
    val resultRDD = HBASERDD.map(tuple => tuple._2)
    val keyValueRDD = resultRDD.map(result => (Bytes.toString(result.getRow()), Bytes.toString(result.getValue(Bytes.toBytes("emp personal data"), Bytes.toBytes("name")))))
    keyValueRDD.collect().foreach(kv => println(kv))
    val PersistRDD = keyValueRDD.map(f => (f._1, "deep learning consultant and my wife "))
   
    // Write to Hbase 
    
    val jobConfig: JobConf = new JobConf(conf, this.getClass)
  //  jobConfig.set("mapreduce.output.fileoutputformat.outputdir", "/user/user01/out")
    jobConfig.setOutputFormat(classOf[TableOutputFormat])
    jobConfig.set(TableOutputFormat.OUTPUT_TABLE, "employee")
    // convert rowkey, psi stats to put and write to hbase table stats column family
    PersistRDD.map { case (k, v) => convertToPut(k, v) }.saveAsHadoopDataset(jobConfig)
  }
  // convert rowkey, stats to put 
  def convertToPut(key: String, stats: String): (ImmutableBytesWritable, Put) = {
    val p = new Put(Bytes.toBytes(key))
    // add columns with data values to put
    p.addColumn(Bytes.toBytes("emp personal data"), Bytes.toBytes("balsouma"), Bytes.toBytes(stats))
   
    (new ImmutableBytesWritable, p)
  }

}

