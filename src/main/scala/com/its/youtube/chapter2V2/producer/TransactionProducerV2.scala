package com.its.youtube.chapter2V2.producer

import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.spark.sql.SparkSession
import org.json4s._
import org.json4s.jackson.JsonMethods._

import java.util.Properties
import scala.io.Source


object TransactionProducerV2 {
  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    val spark = SparkSession.builder
      .appName("KafkaProducerTest")
      .master("local[*]") // Adjust as necessary
      .getOrCreate()

    // Path to the large JSON file
    val filePath = "C:\\Users\\paras\\Downloads\\archive\\transactions\\transactions.txt"

    // Load Avro schema from file
    val schemaFile = "src/main/scala/com/its/youtube/chapter2/producerschema.avsc" // Update with the actual path
    val schemaString = Source.fromFile(schemaFile).getLines().mkString
    val schema = new Schema.Parser().parse(schemaString)

    // Define Kafka properties
    val kafkaProperties = new Properties()
    kafkaProperties.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094")
    kafkaProperties.put("key.serializer", classOf[KafkaAvroSerializer].getName)
    kafkaProperties.put("value.serializer", classOf[KafkaAvroSerializer].getName)
    kafkaProperties.put("schema.registry.url", "http://localhost:8081") // Replace with your schema registry URL

    // Broadcast the schema and Kafka properties
    val broadcastSchema = spark.sparkContext.broadcast(schema)
    val broadcastKafkaProperties = spark.sparkContext.broadcast(kafkaProperties)

    // Read the JSON file
    val jsonFileRDD = spark.sparkContext.textFile(filePath)

    // Process RDD partitions and send to Kafka
    jsonFileRDD.foreachPartition { partition =>
      // Create Kafka producer within the partition scope to ensure serializability
      val producer = new KafkaProducer[String, GenericRecord](broadcastKafkaProperties.value)

      // Define implicit JSON formats within the partition scope
      implicit val formats: Formats = DefaultFormats

      try {
        partition.foreach { line =>
          // Parse JSON line
          val json = parse(line)

          // Convert JSON to Avro record
          val record = new GenericData.Record(broadcastSchema.value)
          record.put("accountNumber", (json \ "accountNumber").extract[String])
          record.put("customerId", (json \ "customerId").extract[String])
          record.put("creditLimit", (json \ "creditLimit").extract[Float])
          record.put("availableMoney", (json \ "availableMoney").extract[Float])
          record.put("transactionDateTime", (json \ "transactionDateTime").extract[String])
          record.put("transactionAmount", (json \ "transactionAmount").extract[Float])
          record.put("merchantName", (json \ "merchantName").extract[String])
          record.put("acqCountry", (json \ "acqCountry").extract[String])
          record.put("merchantCountryCode", (json \ "merchantCountryCode").extract[String])
          record.put("posEntryMode", (json \ "posEntryMode").extract[String])
          record.put("posConditionCode", (json \ "posConditionCode").extract[String])
          record.put("merchantCategoryCode", (json \ "merchantCategoryCode").extract[String])
          record.put("currentExpDate", (json \ "currentExpDate").extract[String])
          record.put("accountOpenDate", (json \ "accountOpenDate").extract[String])
          record.put("dateOfLastAddressChange", (json \ "dateOfLastAddressChange").extract[String])
          record.put("cardCVV", (json \ "cardCVV").extract[String])
          record.put("enteredCVV", (json \ "enteredCVV").extract[String])
          record.put("cardLast4Digits", (json \ "cardLast4Digits").extract[String])
          record.put("transactionType", (json \ "transactionType").extract[String])
          record.put("echoBuffer", (json \ "echoBuffer").extract[String])
          record.put("currentBalance", (json \ "currentBalance").extract[Float])
          record.put("merchantCity", (json \ "merchantCity").extract[String])
          record.put("merchantState", (json \ "merchantState").extract[String])
          record.put("merchantZip", (json \ "merchantZip").extract[String])
          record.put("cardPresent", (json \ "cardPresent").extract[Boolean])
          record.put("posOnPremises", (json \ "posOnPremises").extract[String])
          record.put("recurringAuthInd", (json \ "recurringAuthInd").extract[String])
          record.put("expirationDateKeyInMatch", (json \ "expirationDateKeyInMatch").extract[Boolean])
          record.put("isFraud", (json \ "isFraud").extract[Boolean])

          // Send to Kafka
          val producerRecord = new ProducerRecord[String, GenericRecord]("test-topic-v2", record)
          producer.send(producerRecord, new Callback {
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