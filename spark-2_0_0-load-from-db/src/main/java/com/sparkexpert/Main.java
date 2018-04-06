package com.sparkexpert;

import java.io.Serializable;
import java.util.List;
import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main implements Serializable {

  
	private static final long serialVersionUID = 1L;

	private static final org.apache.log4j.Logger LOGGER = org.apache.log4j.Logger.getLogger(Main.class);

    private static final String MYSQL_CONNECTION_URL = "jdbc:mysql://localhost:3306/malek";
    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PWD = "";

    private static final SparkSession sparkSession =
            SparkSession.builder().master("local[*]").appName("Spark2JdbcDs").getOrCreate();

    public static void main(String[] args) {
        //JDBC connection properties
        final Properties connectionProperties = new Properties();
        connectionProperties.put("user", MYSQL_USERNAME);
        connectionProperties.put("password", MYSQL_PWD);

        final String dbTable =
                "(select id, concat_ws(' ', email, name) as full_name from users) as users_name";

        //Load MySQL query result as Dataset
        Dataset<Row> jdbcDF =
                sparkSession.read()
                        .jdbc(MYSQL_CONNECTION_URL, dbTable, "id", 1, 15, 5, connectionProperties);

        List<Row> employeeFullNameRows = jdbcDF.collectAsList();

        for (Row employeeFullNameRow : employeeFullNameRows) {
            LOGGER.info(employeeFullNameRow);
        }
    }
}
