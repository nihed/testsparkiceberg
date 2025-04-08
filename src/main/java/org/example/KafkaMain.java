package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class KafkaMain {

    public static void main(String[] args) throws InterruptedException, TimeoutException, StreamingQueryException {
        System.out.println("Test Kafka");
        SparkConf conf = new SparkConf()
                .setAppName("Spark Streaming Wiki")
                .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .set("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
                .set("spark.sql.catalog.local.type", "hadoop")
                .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
                .set("spark.sql.catalog.spark_catalog.type", "hadoop")
                .set("spark.sql.catalog.spark_catalog.warehouse", "/tmp/warehouse")
                .set("spark.sql.catalog.local.warehouse", "/tmp/warehouse")

                .setMaster("local[2]");


        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
        sparkSession.sql("create table IF NOT EXISTS testkafka (key string, value string, topic string) using iceberg TBLPROPERTIES('format-version'='2', 'write.metadata.delete-after-commit.enabled'='true', 'write.metadata.previous-versions-max'='10'  )   ");

        Dataset<Row> df = sparkSession
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "fakedata")
                .option("startingOffsets", "earliest")
                .load();
        df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").writeStream()
                .format("iceberg")
                .outputMode("append")
                .trigger(Trigger.ProcessingTime(1, TimeUnit.SECONDS))
                .option("fanout-enabled", "true")
                .option("checkpointLocation", "/tmp/testkafka-cp")
                .toTable("testkafka").awaitTermination();








    }
}
