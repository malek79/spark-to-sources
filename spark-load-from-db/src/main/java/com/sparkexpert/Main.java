package com.sparkexpert;

import java.io.Serializable;
import java.util.List;
import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main implements Serializable {

   
	private static final long serialVersionUID = -5225648975393465115L;

	private static final org.apache.log4j.Logger LOGGER = org.apache.log4j.Logger.getLogger(Main.class);

    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PWD = "";
    private static final String MYSQL_CONNECTION_URL =
            "jdbc:mysql://localhost:3306/malek" ;

	private static final Properties connectionProperties = new Properties();
    
    private static final SparkSession ss = SparkSession.builder().appName("SparkSaveToDb").master("local[*]")
			.getOrCreate();
    
    public static void main(String[] args) {
        //Data source options
    	connectionProperties.put("user", MYSQL_USERNAME);
		connectionProperties.put("password", MYSQL_PWD);

        //Load MySQL query result as DataFrame
        Dataset<Row> jdbcDF = ss.read().jdbc(MYSQL_CONNECTION_URL, "users", connectionProperties);

        List<Row> employeeFullNameRows = jdbcDF.collectAsList();

        for (Row employeeFullNameRow : employeeFullNameRows) {
            LOGGER.info(employeeFullNameRow);
        }
    }
}
