# Java Test: Apache Spark 3.5.5 + Apache Iceberg 1.8.1 Integration
  
This is a basic test project written in Java to explore the integration between Apache Spark 3.5.5 and Apache Iceberg 1.8.1.

The application consumes real-time streaming data from the Wikipedia stream and writes it to Iceberg-backed Parquet files.

added option:
--add-exports java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED