package com.dataGlen

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.struct

object  readKafka extends  App{

  if (args.length < 4) {
    System.err.println("Usage: ReadKafka input_topic_name output_topic_name" +
      "kafka.bootstrap.servers checkpointDirectory")
    System.exit(1)
  }

  val inp_topic = args(0)
  val out_topic = args(1)
  val bootstrapIp = args(2)
  val checkpointDir = args(3)


  //Build spark session for streaming
  val spark = SparkSession
              .builder()
              .appName("ReadKafka using Spark session")
              .getOrCreate()

  //Set the log level
  spark.sparkContext.setLogLevel("WARN")

  //Define Schema for JSON
  val schema = StructType(Seq(
    StructField("TIMESTAMP", StringType),
    StructField("key", StringType),
    StructField("val", LongType )
  ))

  //Connect to Kafka , read stream and parse required columns
  val json_df = spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", bootstrapIp)
  .option("subscribe", inp_topic)
    .option("startingOffsets", "latest")
    .load()
    .select(from_json(col("value").cast("string"), schema)
      .as("value"),col("timestamp").as("kafkatime"))

  //Flatten json_df and extract required columns
  val json_df2 = json_df.selectExpr("value.key","value.val","value.TIMESTAMP","kafkatime") //test

  // group the data by key and perform aggregations
  val grouped_data = json_df2.groupBy(col("key"),
    window(col("kafkatime"), "2 minutes"))
    .agg(count("val").as("count"),
      first("TIMESTAMP").as("TIMESTAMP"),
      sum("val").as("sum"),
      collect_list(col("TIMESTAMP")).as("ts"),
      col("key"),
      collect_list( col("val")).as("vals"),
      mean("val").as("mean")).orderBy("key")

  // prepare key and value for kafka and write it into Kafka
  val write_kafka = grouped_data
    .select(col("key").as("key")
      ,to_json(struct(grouped_data.columns.head,grouped_data.columns.tail:_*)).
        cast("String").as("value"))
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers",bootstrapIp)
    .option("topic", out_topic)
    .option("checkpointLocation", checkpointDir)
    .outputMode("complete")
    .start()

  write_kafka.awaitTermination()

}
