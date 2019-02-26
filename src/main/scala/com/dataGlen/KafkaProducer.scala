package com.dataGlen

import java.time.LocalDateTime
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.errors.{AuthorizationException, OutOfOrderSequenceException, ProducerFencedException}
import scala.util.Random

object KafkaProducer extends App {


  if (args.length < 2) {
    System.err.println("Usage: KafkaProducer input_topic_name " +
      " kafka.bootstrap.servers")
    System.exit(1)
  }

  //read topic name and broker ip
  val topic = args(0)
  val brokerip = args(1)
  val rnd = new Random()

  //Set the properties
  val props: Properties = new Properties()
  props.put("bootstrap.servers", brokerip)
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")

  //create Kafka producer
  val myProducer = new KafkaProducer[String,Array[Byte]](props)

  try {

    while(true)
     {
       //Send records at frequency 5 seconds
       val in_key = "key"+ rnd.nextInt(5)
       val in_value = rnd.nextInt(1000)
       val in_time = LocalDateTime.now().toString
       val msg = "{\"TIMESTAMP\": \"" + in_time + "\", \"val\": "+ in_value+", \"key\": \""+in_key+"\"}"
       val data = new ProducerRecord[String, Array[Byte]](topic, brokerip , msg.getBytes)
       myProducer.send(data)
       println("Message sent to " + topic)
       Thread.sleep(5000)
    }

  } catch {
    case e: ProducerFencedException =>
      println("Not able to send data " + e)
      myProducer.close()

    case e: OutOfOrderSequenceException =>
      println("Not able to send data " + e)
      myProducer.close()

    case e: AuthorizationException =>
      println("Not able to send data " + e)
      myProducer.close()

    case e: KafkaException =>
      println("Exception " + e)
      myProducer.abortTransaction()

  }

}
