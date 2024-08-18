package com.its.youtube.chapter2V2.producer


import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties
object KafkaProducerTest {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    val topic = "test-topic"

    try {
      val record = new ProducerRecord[String, String](topic, "key", "test message")
      producer.send(record)
      print("hello")
    } finally {
      producer.close()
    }
  }
}

