package com.sparkexpert;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

//http://codereview.stackexchange.com/questions/56641/producing-a-sorted-wordcount-with-spark

//export HADOOP_CONF_DIR=/etc/hadoop/conf
//java -cp ./target/hbase-spark-playground-1.0-SNAPSHOT.jar spark.examples.SparkToHBase local[2]
//
//hadoop fs -put README.md
//spark-submit --class spark.examples.SparkToHBase --master local[8]  ./target/hbase-spark-playground-1.0-SNAPSHOT.jar
//FIXME: La ejecucción no comienza y da el siguiente error:
//       WARN hdfs.BlockReaderLocal: The short-circuit local reads feature cannot be used because libhadoop cannot be loaded.

// spark-submit --verbose --class spark.examples.SparkToHBase --master yarn-client  --num-executors 1 --driver-memory 1G  --executor-memory 1G  --executor-cores 1  --driver-class-path /usr/lib/hbase/lib/hbase-protocol-0.98.0.2.1.1.0-385-hadoop2.jar  ./target/hbase-spark-playground-1.0-SNAPSHOT.jar yarn-client 

//spark-submit --class spark.examples.SparkToHBase --master yarn-cluster  --executor-memory 200m --num-executors 1 ./target/hbase-spark-playground-1.0-SNAPSHOT.jar yarn-cluster
//FIXME: el submit se ejecuta y lee y guarda en HBase correctamente. Sin embargo la tarea Spark termina como Failed en distributedFinalState:.
//Viendo los logs se puede observar que hay un problema con el DNS
//ERROR mapreduce.TableInputFormatBase: Cannot resolve the host name for sandbox.hortonworks.com/192.168.149.133 because of javax.naming.NameNotFoundException: DNS name not found [response code 3]; remaining name '133.149.168.192.in-addr.arpa'

public class SparkToHBase {

	private final static String tableName = "employee";
	private final static String columnFamily = "emp professional data";

	private static final SparkSession ss = SparkSession.builder().appName("SparkSaveToDb").master("local[*]")
			.getOrCreate();
	
	public static void main(String[] args) throws Exception {

		

		Configuration conf = HBaseConfiguration.create();

		conf.set(TableInputFormat.INPUT_TABLE, tableName);

		// new Hadoop API configuration
		Job newAPIJobConfiguration = Job.getInstance(conf);
		newAPIJobConfiguration.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, tableName);
		newAPIJobConfiguration.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);

	
		HBaseAdmin hBaseAdmin = null;
		try {
			hBaseAdmin = new HBaseAdmin(conf);
			if (hBaseAdmin.isTableAvailable(tableName)) {
				System.out.println("Table " + tableName + " is available.");
			} else {
				System.out.println("Table " + tableName + " is not available.");
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			hBaseAdmin.close();
		}

		System.out.println("-----------------------------------------------");
		// readTable(conf, mode);
		readTableJava(conf);
		System.out.println("-----------------------------------------------");
		writeRowNewHadoopAPI(newAPIJobConfiguration.getConfiguration());
		System.out.println("-----------------------------------------------");
		readTableJava(conf);
		System.out.println("-----------------------------------------------");
		// System.exit(0);
		ss.sparkContext().stop();

	}

	private static void readTableJava(Configuration conf) {

		RDD<Tuple2<ImmutableBytesWritable, Result>> hBaseRDD = ss.sparkContext().newAPIHadoopRDD(conf, TableInputFormat.class,
				org.apache.hadoop.hbase.io.ImmutableBytesWritable.class, org.apache.hadoop.hbase.client.Result.class);
		long count = hBaseRDD.count();
		System.out.println("Number of register in hbase table: " + count);
		// sc.stop();
	}

	
	private static void writeRowNewHadoopAPI(Configuration conf) {

		// JavaSparkContext sparkContext = new JavaSparkContext(mode, "write data to
		// HBase");
		RDD<String> records = ss.sparkContext().textFile("README.md", 1);
		// FIXME: mirar como quitar la carga de un texto arbitrario para crear un
		// JavaRDD
		JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = records.toJavaRDD()
				.mapToPair(new PairFunction<String, ImmutableBytesWritable, Put>() {
					@Override
					public Tuple2<ImmutableBytesWritable, Put> call(String t) throws Exception {
						Put put = new Put(Bytes.toBytes("3"));
						put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("file"), Bytes.toBytes("value3"));

						return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);
					}
				});

		hbasePuts.saveAsNewAPIHadoopDataset(conf);
		// sparkContext.stop();

	}

}