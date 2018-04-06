package com.hbase

import org.apache.hadoop.conf.Configuration

//import org.apache.hadoop.hbase.util.HBaseConfTool

import org.apache.hadoop.hbase.HBaseConfiguration

import org.apache.hadoop.hbase.client.Connection

import org.apache.hadoop.hbase.client.ConnectionFactory

import org.apache.hadoop.hbase.TableName

import org.apache.hadoop.hbase.util.Bytes

import org.apache.hadoop.hbase.client.Put

import org.apache.hadoop.hbase.client.Get

object tessst {

  val conf: Configuration = HBaseConfiguration.create()

  def main(args: Array[String]): Unit = {

    val connection: Connection = ConnectionFactory.createConnection(conf)

    val table = connection.getTable(TableName.valueOf("employee"))

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

  }

}

