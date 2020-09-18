package com.bigdata.spark.B_examples_sparksql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.*;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

public class GroupByFromJson {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "D:\\Libs\\hadoop-common-2.2.0-bin-master");

        org.apache.spark.sql.types.StructType schema = new org.apache.spark.sql.types.StructType()
                .add("id", DataTypes.IntegerType)
                .add("first_name", DataTypes.StringType)
                .add("last_name", DataTypes.StringType)
                .add("email", DataTypes.StringType)
                .add("country", DataTypes.StringType)
                .add("price", DataTypes.DoubleType)
                .add("product", DataTypes.StringType);

        SparkSession sparkSession = SparkSession.builder().master("local").appName("Product Sample").getOrCreate();

        Dataset<Row> rawDS = sparkSession.read().option("multiline", true).schema(schema).json("D:\\eclipse-workspace\\spark-core\\src\\product.json");

        //rawDS.show();

        Dataset<Row> countrySales = rawDS.groupBy("country").sum("price");

        //countrySales.show();

        Dataset<Row> countryProductSales = rawDS.groupBy("country", "product").sum("price");

        Dataset<Row> countryAvg = rawDS.groupBy("country").avg("price");

        countryAvg.show();

        Dataset<Row> countryProductSalesCount = rawDS.groupBy("country", "product").count();
        //countryProductSalesCount.sort(functions.desc("count")).show();




    }
}
