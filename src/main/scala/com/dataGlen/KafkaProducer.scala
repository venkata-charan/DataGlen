package com.dataGlen

import java.time.LocalDateTime
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.errors.{AuthorizationException, OutOfOrderSequenceException, ProducerFencedException}
import scala.util.Random

object KafkaProducer extends App {


  val topic = args(0)
  val brokerip = args(1)
  val rnd = new Random()

  val props: Properties = new Properties()
  props.put("bootstrap.servers", brokerip)
  //props.put("transactional.id", "my-transactional-id")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val myProducer = new KafkaProducer[String, String](props)

  //myProducer.initTransactions()

  try {
    //myProducer.beginTransaction()
    while(true)
     {
       println("trying to send messages")
       val in_key = "key"+ rnd.nextInt(5)
       val in_value = rnd.nextInt(1000)
       val in_time = LocalDateTime.now().toString
       val msg = "{\"TIMESTAMP\": \"" + in_time + "\", \"val\": "+ in_value+", \"key\": \""+in_key+"\"}"
       val data = new ProducerRecord[String, String](topic, brokerip , msg)
       myProducer.send(data)
      // myProducer.commitTransaction()
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
