package com.bigdata.spark.B_examples_sparksql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class WithColumn {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "D:\\Libs\\hadoop-common-2.2.0-bin-master");

        SparkSession sparkSession = SparkSession.builder().master("local").appName("First Exam").getOrCreate();
        Dataset<Row> rawDS = sparkSession.read().option("header", true).csv("D:\\eclipse-workspace\\spark-core\\src\\movieswt.csv");

        Dataset<Row> wcDS = rawDS.withColumn("Movie Name", rawDS.col("title"));

        wcDS.show();

        //rawDS.show();

        //rawDS.printSchema();

        //Dataset<Row> selectDS = rawDS.select("_c0", "_c1");

        //selectDS.show();
    }
}
