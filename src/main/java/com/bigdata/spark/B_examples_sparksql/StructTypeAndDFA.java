package com.bigdata.spark.B_examples_sparksql;

import javafx.scene.chart.PieChart;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class StructTypeAndDFA {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "D:\\Libs\\hadoop-common-2.2.0-bin-master");

        org.apache.spark.sql.types.StructType schema = new org.apache.spark.sql.types.StructType().add("movieId", DataTypes.IntegerType)
                .add("title", DataTypes.StringType)
                .add("genres", DataTypes.StringType);

        SparkSession sparkSession = SparkSession.builder().master("local").appName("First Exam").getOrCreate();

        Dataset<Row> rawDS = sparkSession.read().option("header", true).schema(schema).csv("D:\\eclipse-workspace\\spark-core\\src\\movieswt.csv");

        Dataset<Row> selDS = rawDS.select("title", "genres");

        //Spark SQL DataFrame Api
        Dataset<Row> actionFilter = selDS.filter("genres = 'Action' or genres == 'Comedy'");

        actionFilter.show();

        //Dataset<Row> lessThanTen = selDS.filter("movieId < 10").sort("genres");

        //lessThanTen.show();

        //actionFilter.show();

        //selDS.show();

        //rawDS.printSchema();

        //Dataset<Row> selectDS = rawDS.select("_c0", "_c1");

        //selectDS.show();
    }
}
