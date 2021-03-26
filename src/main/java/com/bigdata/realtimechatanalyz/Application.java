package com.bigdata.realtimechatanalyz;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;
import java.util.Iterator;

public class Application {
    public static void main(String[] args) throws StreamingQueryException {
        System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master");
        SparkSession sparkSession = SparkSession.builder().appName("Spark Steam Chat Listener")
                .master("local").getOrCreate();

        Dataset<Row> rowDataset = sparkSession.readStream().format("socket")
                .option("host", "localhost")
                .option("port", "8005")
                .load();


        Dataset<String> stringDataset = rowDataset.as(Encoders.STRING());

        Dataset<String> splitString = stringDataset.flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(s.split(" ")).iterator(), Encoders.STRING());

        Dataset<Row> groupedStringData = splitString.groupBy("value").count().sort(functions.desc("count"));

        StreamingQuery streamingQuery = groupedStringData.writeStream().format("console").outputMode("complete").start();

        streamingQuery.awaitTermination();

    }

}
