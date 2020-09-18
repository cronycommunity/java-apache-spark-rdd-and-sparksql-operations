package com.bigdata.spark.A_examples_rdd.B_maptransformation;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;

import java.util.Arrays;
import java.util.Iterator;

public class FlatMap {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "D:\\Libs\\hadoop-common-2.2.0-bin-master");

        JavaSparkContext javaSparkContext = new JavaSparkContext("local", "FlatMap Spark");

        JavaRDD<String> rawData = javaSparkContext.textFile("D:\\eclipse-workspace\\spark-core\\src\\movies.csv");

        JavaRDD<String> stringJavaRDDflatMap = rawData.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception{
                return Arrays.asList(s.split(",")).iterator();
            }
        });

        stringJavaRDDflatMap.foreach(new VoidFunction<String>() {
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

    }
}
