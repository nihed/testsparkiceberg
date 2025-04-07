package org.example;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.r.BaseRRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.ReceiverInputDStream;
import org.apache.spark.streaming.receiver.Receiver;
import scala.Tuple2;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.Thread.sleep;

/**
 * Sample application to collect streaming data from Wikimedia for a processing on Spark
 *
 */
public class Main
{
    public static void main( String[] args ) throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setAppName("Spark Streaming Wiki")
                .set("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                //.set("spark.sql.catalog.spark_catalog","org.apache.iceberg.spark.SparkSessionCatalog")
               // .set("spark.sql.catalog.spark_catalog.type","hive")
                .set("spark.sql.catalog.local","org.apache.iceberg.spark.SparkCatalog")
                .set("spark.sql.catalog.local.type","hadoop")
                .set("spark.sql.catalog.local.warehouse","/tmp/warehouse")

                .setMaster("local[2]");
        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
        StreamingContext streamingContext = new StreamingContext(sparkSession.sparkContext(), Durations.seconds(1));
        JavaStreamingContext jssc = new JavaStreamingContext(streamingContext);


        sparkSession.sql("create table testtable (word string) using iceberg TBLPROPERTIES('format-version'='2')   location '/Users/nihedmbarek/IdeaProjects/SparkStreamIceberg/spark-warehouse/testtable'");
        System.out.println("-- ---- ---- --- ");
        sparkSession.sql("show create table testtable").foreach(new ForeachFunction<Row>() {
            @Override
            public void call(Row row) throws Exception {
                System.out.println(row.toString());
            }
        });
        JavaReceiverInputDStream<String> inputDStream = jssc.receiverStream(new MyWikiReceiver(StorageLevel.DISK_ONLY()));


        inputDStream.count().print();

        Function<String, String> extractWiki = new Function<String, String>() {
            @Override
            public String call(String t) throws Exception {
                Pattern pattern = Pattern.compile(",\"wiki\":\"(.*?)\",");
                Matcher matcher = pattern.matcher(t);
                if (matcher.find())
                {
                    return matcher.group(1);
                }
                return null;
            }
        };

       inputDStream.map(extractWiki).map(new Function<String, JavaRecord>() {
           @Override
           public JavaRecord call(String s) throws Exception {
               return new JavaRecord(s);
           }
       }).foreachRDD(new VoidFunction<JavaRDD<JavaRecord>>() {
           @Override
           public void call(JavaRDD<JavaRecord> javaRecordJavaRDD) throws Exception {
               Dataset<Row> dataFrame = sparkSession.createDataFrame(javaRecordJavaRDD, JavaRecord.class);

dataFrame.writeTo("testtable").append();


           }
       });




        jssc.start();
        jssc.awaitTermination();

    }
    public static class JavaRecord implements java.io.Serializable {
        private String word;

        public JavaRecord() {
        }

        public JavaRecord(String word) {
            this.word = word;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }
    }


    private static class MyWikiReceiver extends Receiver<String> {
        public MyWikiReceiver(StorageLevel storageLevel) {
            super(storageLevel);
        }

        public void onStart() {
            StringBuilder result = new StringBuilder();
            URL url = null;
            try {
                url = new URL("https://stream.wikimedia.org/v2/stream/recentchange");

                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("GET");
                BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                String line;
                while ((line = rd.readLine()) != null) {
                    store(line);
                }
                rd.close();
            } catch (MalformedURLException | ProtocolException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public void onStop() {
        }
    }
}