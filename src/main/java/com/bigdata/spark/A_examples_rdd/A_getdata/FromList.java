package com.bigdata.spark.A_examples_rdd.A_getdata;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.Arrays;
import java.util.List;

public class FromList {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "D:\\Libs\\hadoop-common-2.2.0-bin-master");

        JavaSparkContext javaSparkContext = new JavaSparkContext("local", "From List Spark");

        List<String> data = Arrays.asList("big Data", "Elastic Search", "hadoop");
        JavaRDD<String> secondData = javaSparkContext.parallelize(data);

        System.out.println("Sum:" + secondData.count());
        System.out.println("First Data:" + secondData.first());
    }

}
