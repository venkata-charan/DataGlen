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
    .option("startingOffsets", "latest")
    .load() //test

  val df1 = df.select("CAST(key AS STRING) key", "CAST(value AS STRING) value")
 df1.printSchema()
  df1.show()
  val query = df1.writeStream
    .outputMode("append")
    .format("console")
    .start()

  query.awaitTermination()

}
