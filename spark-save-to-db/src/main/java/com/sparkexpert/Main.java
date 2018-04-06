package com.sparkexpert;

import java.io.Serializable;
import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main implements Serializable {

	private static final long serialVersionUID = 1L;

	private static final String MYSQL_USERNAME = "root";
	private static final String MYSQL_PWD = "";
	private static final String MYSQL_CONNECTION_URL = "jdbc:mysql://localhost:3306/malek?user=" + MYSQL_USERNAME
			+ "&password=" + MYSQL_PWD;

	private static final Properties connectionProperties = new Properties();

	private static final SparkSession ss = SparkSession.builder().appName("SparkSaveToDb").master("local[*]")
			.getOrCreate();

	public static void main(String[] args) {
		// Sample data-frame loaded from a JSON file
		connectionProperties.put("user", MYSQL_USERNAME);
		connectionProperties.put("password", MYSQL_PWD);

		Dataset<Row> usersDf = ss.read().json("/home/malek/workspace/SparkApps-master/spark-save-to-db/src/main/resources/users.json");

		usersDf.write().jdbc(MYSQL_CONNECTION_URL, "users", connectionProperties);
	}
}
