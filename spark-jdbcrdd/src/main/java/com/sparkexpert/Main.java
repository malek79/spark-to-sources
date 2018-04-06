package com.sparkexpert;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.JdbcRDD;
import org.apache.spark.sql.SparkSession;

import scala.reflect.ClassManifestFactory$;
import scala.runtime.AbstractFunction0;
import scala.runtime.AbstractFunction1;

public class Main implements Serializable {

	private static final long serialVersionUID = 4371277950430896291L;

	private static final org.apache.log4j.Logger LOGGER = org.apache.log4j.Logger.getLogger(Main.class);

	private static final SparkSession ss = SparkSession.builder().appName("SparkSaveToDb").master("local[*]")
			.getOrCreate();

	private static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
	private static final String MYSQL_CONNECTION_URL = "jdbc:mysql://localhost:3306/malek";
	private static final String MYSQL_USERNAME = "root";
	private static final String MYSQL_PWD = "";

	public static void main(String[] args) {

		
		DbConnection dbConnection = new DbConnection(MYSQL_DRIVER, MYSQL_CONNECTION_URL, MYSQL_USERNAME, MYSQL_PWD);

		// Load data from MySQL
		JdbcRDD<Object[]> jdbcRDD = new JdbcRDD<>(ss.sparkContext(), dbConnection,
				"select * from users where id >= ? and id <= ?", 1, 10, 5, new MapResult(),
				ClassManifestFactory$.MODULE$.fromClass(Object[].class));

		// Convert to JavaRDD
		JavaRDD<Object[]> javaRDD = JavaRDD.fromRDD(jdbcRDD, ClassManifestFactory$.MODULE$.fromClass(Object[].class));

		// Join first name and last name
		List<String> employeeFullNameList = javaRDD.map(record -> {
			return record[2] + " " + record[3];

		}).collect();

		for (String fullName : employeeFullNameList) {
			LOGGER.info(fullName);
		}
	}

	static class DbConnection extends AbstractFunction0<Connection> implements Serializable {

		
		private static final long serialVersionUID = 1L;
		
		private String driverClassName;
		private String connectionUrl;
		private String userName;
		private String password;

		public DbConnection(String driverClassName, String connectionUrl, String userName, String password) {
			this.driverClassName = driverClassName;
			this.connectionUrl = connectionUrl;
			this.userName = userName;
			this.password = password;
		}

		@Override
		public Connection apply() {
			try {
				Class.forName(driverClassName);
			} catch (ClassNotFoundException e) {
				LOGGER.error("Failed to load driver class", e);
			}

			Properties properties = new Properties();
			properties.setProperty("user", userName);
			properties.setProperty("password", password);

			Connection connection = null;
			try {
				connection = DriverManager.getConnection(connectionUrl, properties);
			} catch (SQLException e) {
				LOGGER.error("Connection failed", e);
			}

			return connection;
		}
	}

	static class MapResult extends AbstractFunction1<ResultSet, Object[]> implements Serializable {

		
		private static final long serialVersionUID = 1L;

		public Object[] apply(ResultSet row) {
			return JdbcRDD.resultSetToObjectArray(row);
		}
	}
}
