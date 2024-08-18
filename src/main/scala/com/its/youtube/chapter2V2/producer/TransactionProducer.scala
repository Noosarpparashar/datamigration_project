package com.its.youtube.chapter2V2.producer


import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.sql.SparkSession
import java.util.Properties

object TransactionProducer {
  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    val spark = SparkSession.builder
      .appName("KafkaProducerTest")
      .master("local[*]") // Adjust as necessary
      .getOrCreate()

    // Path to the large text file
    val filePath = "C:\\Users\\paras\\Downloads\\archive\\transactions\\transactions.txt"

    // Read the text file
    val textFileRDD = spark.sparkContext.textFile(filePath)

    // Define Kafka properties
    val kafkaProperties = new Properties()
    kafkaProperties.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094")
    kafkaProperties.put("key.serializer", classOf[StringSerializer].getName)
    kafkaProperties.put("value.serializer", classOf[StringSerializer].getName)

    // Function to create a KafkaProducer instance
    def createProducer(): KafkaProducer[String, String] = new KafkaProducer[String, String](kafkaProperties)

    // Process RDD partitions and send to Kafka
    textFileRDD.foreachPartition { partition =>
      val producer = createProducer()
      try {
        partition.foreach { line =>
          val record = new ProducerRecord[String, String]("test-topic", line)
          producer.send(record, new Callback {
            override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
              if (exception != null) {
                println(s"Error sending message: ${exception.getMessage}")
              } else {
                println(s"Message sent to topic ${metadata.topic()} at partition ${metadata.partition()} with offset ${metadata.offset()}")
              }
            }
          })
        }
      } finally {
        producer.close()
      }
    }

    // Stop the SparkSession
    spark.stop()

  }
}




