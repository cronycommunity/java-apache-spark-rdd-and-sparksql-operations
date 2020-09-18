package com.bigdata.spark.B_examples_sparksql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class SQLFilter {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "D:\\Libs\\hadoop-common-2.2.0-bin-master");

        StructType schema = new StructType().add("movieId", DataTypes.IntegerType)
                .add("title", DataTypes.StringType)
                .add("genres", DataTypes.StringType);

        SparkSession sparkSession = SparkSession.builder().master("local").appName("First Exam").getOrCreate();

        Dataset<Row> rawDS = sparkSession.read().option("header", true).schema(schema).csv("D:\\eclipse-workspace\\spark-core\\src\\movieswt.csv");

        Dataset<Row> selDS = rawDS.select("movieId", "genres");

        //Spark SQL DataFrame Api
        //Dataset<Row> actionFilter = selDS.filter(selDS.col("genres").equalTo("Action").and(selDS.col("movieId").gt(204))); // lt : lower than

        Dataset<Row> actionFilter = selDS.filter(selDS.col("genres").contains("Action").and(selDS.col("movieId").gt(204))
                .and(selDS.col("movieId").lt(300))); // lt : lower than

        actionFilter.show();
    }
}
