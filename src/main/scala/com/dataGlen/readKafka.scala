package com.dataGlen

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

object  readKafka extends  App{

  val spark = SparkSession
              .builder()
              .appName("ReadKafka using Spark session")
              .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")


  val schema = StructType(Seq(
    StructField("TIMESTAMP", StringType),
    StructField("key", StringType),
    StructField("val", LongType )
  ))


  val json_df = spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "ip-172-31-38-146.ec2.internal:6667")
  .option("subscribe", "walmart_topic")
    .option("startingOffsets", "latest")
    .load()
    .select(from_json(col("value").cast("string"), schema).as("value"))

  val json_df2 = json_df.selectExpr("value.key","value.val","value.TIMESTAMP")

  val df1 = json_df2.groupBy("key").agg(count("val").as("count"),
    current_timestamp().as("TIMESTAMP"),
    sum("val").as("sum"),
    collect_list("TIMESTAMP").as("ts"),
    col("key"),
    collect_list("val").as("vals"),
    mean("val").as("mean")).orderBy("key")

  val query = df1.writeStream
    .outputMode("append")
    .format("console")
    .start()

  query.awaitTermination()

}
