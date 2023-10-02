package com.miu.bdt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hbase.thirdparty.io.netty.handler.codec.string.StringDecoder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import com.esotericsoftware.minlog.Log;

import scala.Tuple2;

import java.io.IOException;
import java.util.*;

public class SparkStreamingToHBase {

    private static final String TABLE_NAME = "security_logs";
    private static final String COLUMN_FAMILY = "security_events";

    public static void writeToHBase(String timestamp, String logType, String sourceIP, String user, String actionTaken) throws IOException {
        Configuration config = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(config);
        Admin admin = connection.getAdmin();

        try {
        	  TableName tableName = TableName.valueOf(TABLE_NAME);
              Table table = connection.getTable(tableName);

              Put put = new Put(Bytes.toBytes(timestamp));
              put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("log_type"), Bytes.toBytes(logType));
              put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("source_ip"), Bytes.toBytes(sourceIP));
              put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("user"), Bytes.toBytes(user));
              put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("action_taken"), Bytes.toBytes(actionTaken));

              table.put(put);
              table.close();
           
           
        } finally {
            connection.close();
            admin.close();
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        // Set up SparkConf and create JavaStreamingContext
        SparkConf sparkConf = new SparkConf().setAppName("SparkStreamingToHBase").setMaster("local");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        // Set up Kafka parameters
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "192.168.1.66:9092");
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("group.id", "my_consumer_group");

        // Set up topics
        Set<String> topics = Collections.singleton("security_logs_stream");

        // Create Kafka DStream      
        JavaInputDStream<ConsumerRecord<String, String>> stream =
        		  KafkaUtils.createDirectStream(
        				  jssc,
        		    LocationStrategies.PreferConsistent(),
        		    ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
        		  );
        
        JavaPairDStream<String, String> logs =  stream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));
        // Store data in HBase
        logs.foreachRDD(rdd -> {
            rdd.foreachPartition(partition -> {
                while (partition.hasNext()) {
                	try {
                		String log = partition.next()._2();
                        String[] logAttributes = log.split(" ");
                        String timestamp = logAttributes[0];
                        String logType = logAttributes[1];
                        String sourceIP = logAttributes[2];
                        String user = logAttributes[3];
                        String actionTaken = logAttributes[4];
                        writeToHBase(timestamp, logType, sourceIP, user, actionTaken);
					} catch (Exception e) {
						e.printStackTrace();
						System.out.println("Skipping the record");
						
					}
                    
                }
            });
        });

        // Start the streaming context
        jssc.start();
        jssc.awaitTermination();
    }
}

