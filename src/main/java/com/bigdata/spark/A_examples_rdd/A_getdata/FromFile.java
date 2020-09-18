package com.bigdata.spark.A_examples_rdd.A_getdata;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class FromFile {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "D:\\Libs\\hadoop-common-2.2.0-bin-master");

        JavaSparkContext javaSparkContext = new JavaSparkContext("local", "From File Spark");

        JavaRDD<String> firstData = javaSparkContext.textFile("D:\\eclipse-workspace\\spark-core\\src\\firstdata.txt");
        System.out.println(firstData.count());
        System.out.println(firstData.first());
    }

}
