package com.dataGlen

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.struct

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
    .select(from_json(col("value").cast("string"), schema)
      .as("value"),col("timestamp").as("kafkatime"))

  val json_df2 = json_df.selectExpr("value.key","value.val","value.TIMESTAMP","kafkatime") //test

  val df1 = json_df2.groupBy(col("key"))
    //window(col("kafkatime"),
     // "2 minutes","30 seconds"))
    .agg(count("val").as("count"),
      first("TIMESTAMP"),
      sum("val").as("sum"),
      collect_list(col("TIMESTAMP")).as("ts"),
      col("key"),
      collect_list( col("val")).as("vals"),
      mean("val").as("mean")).orderBy("key")

  val query = df1
    .select(col("key").as("Key")
      ,to_json(struct(df1.columns.head,df1.columns.tail:_*)).
        cast("String").as("Value"))
    .writeStream
    .outputMode("complete")
    .format("console")
    .start()

  query.awaitTermination()

}
