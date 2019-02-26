package com.dataGlen

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.time.LocalDateTime
import org.apache.spark.sql.streaming.Trigger
import scala.concurrent.duration._
import scala.util.Random

object KafkaProducer extends App {


  if (args.length < 2) {
    System.err.println("Usage: ReadKafka topic_name " +
      "kafka.bootstrap.servers ")
    System.exit(1)
  }

  val inp_topic = args(0)
  val bootstrapIp = args(2)


  //Build spark session for streaming
  val spark = SparkSession
    .builder()
    .appName("WriteKafka using Spark session")
    .getOrCreate()

  //Set the log level
  spark.sparkContext.setLogLevel("WARN")

  //parse Data key String , (key ,value , timestamp as JSON)
  val rnd = new Random()

  val in_key = "key"+ rnd.nextInt(5)
  val in_value = rnd.nextInt(1000)
  val in_time = LocalDateTime.now().toString
  case class input_data(TIMESTAMP:String,value:Int,key:String)

  val in_df = spark.createDataFrame(Seq(input_data(in_time,in_value,in_key)))

  // prepare key and value for kafka and write it into Kafka
  val write_kafka = in_df
    .select(col("key").as("key")
      ,to_json(struct(in_df.columns.head,in_df.columns.tail:_*)).
        cast("String").as("value"))
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers",bootstrapIp)
    .option("topic", inp_topic)
    .outputMode("append")
    .trigger(Trigger.ProcessingTime(30.seconds))
    .start()

  write_kafka.awaitTermination()

}
