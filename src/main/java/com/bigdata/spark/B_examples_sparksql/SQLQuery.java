package com.bigdata.spark.B_examples_sparksql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

public class SQLQuery {
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

        Dataset<Row> rawDS = sparkSession.read().schema(schema).option("multiline", true).json("D:\\eclipse-workspace\\spark-core\\src\\product.json");

        //Session View
        rawDS.createOrReplaceTempView("product");

        Dataset<Row> sqlDS = sparkSession.sql("select * from product where country ='France' or country = 'Brazil'");

        sqlDS.show();

    }
}
