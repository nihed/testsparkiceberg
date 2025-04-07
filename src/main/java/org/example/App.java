package org.example;



import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
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

/**
 * Sample application to collect streaming data from Wikimedia for a processing on Spark
 *
 */
public class App
{
    public static void main( String[] args ) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("Spark Streaming Wiki").setMaster("local[2]");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
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
        inputDStream.filter(s -> s.startsWith("data: {")).map(extractWiki).mapToPair(s -> new Tuple2<>(s,1)).reduceByKey((a, b) -> a + b).print();

        jssc.start();
        jssc.awaitTermination();

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