Requirement:
Request you to create your own kafka producer for creating messages in below format.
Write a Spark Structured Streaming app to extract JSON messages from kafka broker and aggregate them over a window of 2 mins.

JSON messages are being sent to five different keys- “Key1" to “Key5"
message format: key: Key1 val: {"TIMESTAMP": "2017-02-25T04:44:18", "val": 0, "key": "Key1”} 
frequency of message: 30 seconds
for each key, aggregate the values coming over 2 min window period and compute sum and mean of values received in those 2 mins

You can verify if your aggregation is correct at the aggregation topic “test_aggregated”
Aggregation message format: key: Key3 val: {"count": 4, "TIMESTAMP": "2017-02-25T04:42:00", "sum": 9, "ts": ["2017-02-25T04:42:48", "2017-02-25T04:42:20", "2017-02-25T04:43:24", "2017-02-25T04:43:48"], "key": "Key3", "vals": [4, 1, 1, 3], "mean": 2.25}


Solution:

•	Use spark structured Streaming to read json logs from kafka and parse it.
•	Do aggregations and write the stream back into Kafka



Submit:

Jar files needed:
spark-1.0-SNAPSHOT-jar-with-dependencies.jar
spark-sql-kafka-0-10_2.11-2.3.0.jar

press ctrl+c to stop the producer and consumers

Run kafka Producer: Press (ctrl+c to stop )
scala -classpath spark-1.0-SNAPSHOT-jar-with-dependencies.jar com.dataGlen.KafkaProducer  (provide input topic name) (bootstrap server ip)

Run kafka Consumer: (let it run to continuously read data from kafka)
spark-submit --jars spark-sql-kafka-0-10_2.11-2.3.0.jar --class com.dataGlen.readKafka --master local[*] target/spark-1.0-SNAPSHOT-jar-with-dependencies.jar (provide input topic name) (output topic name) (bootstrap server ip) (checkpoint directory location)









