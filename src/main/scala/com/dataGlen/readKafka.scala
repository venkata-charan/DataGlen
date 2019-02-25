package com.dataGlen

import org.apache.spark.sql.SparkSession

object  readKafka extends  App{

  val spark = SparkSession
              .builder()
              .appName("ReadKafka using Spark session")
              .getOrCreate()

  val df = spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "ip-172-31-38-146.ec2.internal:6667")
  .option("subscribe", "walmart_topic")
  .option("startingOffsets", "earliest")
  .load() //test

  val df1 = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

  val query = df1.writeStream
    .outputMode("Append")
    .format("console")
    .start()

  query.awaitTermination()

}
