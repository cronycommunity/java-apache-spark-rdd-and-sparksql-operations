package com.bigdata.spark.A_examples_rdd.C_actions;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Take {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "D:\\Libs\\hadoop-common-2.2.0-bin-master");

        JavaSparkContext javaSparkContext = new JavaSparkContext("local", "Actions Spark");

        JavaRDD<String> rawData = javaSparkContext.textFile("D:\\eclipse-workspace\\spark-core\\src\\movies.csv");

        System.out.println("Data : " + rawData.take(3));

    }
}
