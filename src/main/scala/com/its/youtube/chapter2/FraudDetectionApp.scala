package com.its.youtube.chapter2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
//import org.apache.spark.sql.avro._
import org.apache.kafka.common.serialization.StringDeserializer

object FraudDetectionApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("FraudDetectionApp")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._

    // Define Kafka parameters
    val kafkaParams = Map[String, String](
      "kafka.bootstrap.servers" -> "localhost:9092,localhost:9093,localhost:9094",
      "subscribe" -> "test-topic",
      "startingOffsets" -> "earliest",
      "value.deserializer" -> "io.confluent.kafka.serializers.KafkaAvroDeserializer",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "schema.registry.url" -> "http://localhost:8081"
    )

    // Read stream from Kafka
    val df = spark.readStream
      .format("kafka")
      .options(kafkaParams)
      .load()

    // Define schema for parsing the JSON data
    val avroSchema = new StructType()
      .add("accountNumber", StringType)
      .add("customerId", StringType)
      .add("creditLimit", FloatType)
      .add("availableMoney", FloatType)
      .add("transactionDateTime", StringType)
      .add("transactionAmount", FloatType)
      .add("merchantName", StringType)
      .add("acqCountry", StringType)
      .add("merchantCountryCode", StringType)
      .add("posEntryMode", StringType)
      .add("posConditionCode", StringType)
      .add("merchantCategoryCode", StringType)
      .add("currentExpDate", StringType)
      .add("accountOpenDate", StringType)
      .add("dateOfLastAddressChange", StringType)
      .add("cardCVV", StringType)
      .add("enteredCVV", StringType)
      .add("cardLast4Digits", StringType)
      .add("transactionType", StringType)
      .add("echoBuffer", StringType)
      .add("currentBalance", FloatType)
      .add("merchantCity", StringType)
      .add("merchantState", StringType)
      .add("merchantZip", StringType)
      .add("cardPresent", BooleanType)
      .add("posOnPremises", StringType)
      .add("recurringAuthInd", StringType)
      .add("expirationDateKeyInMatch", BooleanType)
      .add("isFraud", BooleanType)

    // Parse and transform transactions
    val transactions = df.selectExpr("CAST(value AS STRING) as json")
      .select(from_json(col("json"), avroSchema).as("data"))
      .select(
        col("data.accountNumber"),
        col("data.customerId"),
        col("data.creditLimit"),
        col("data.availableMoney"),
        col("data.transactionAmount"),
        col("data.posEntryMode"),
        col("data.posConditionCode"),
        col("data.cardCVV"),
        col("data.enteredCVV"),
        col("data.cardLast4Digits"),
        col("data.currentBalance"),
        col("data.cardPresent"),
        col("data.posOnPremises"),
        col("data.recurringAuthInd"),
        col("data.expirationDateKeyInMatch")
      )

    // Apply fraud detection logic
    val flaggedTransactions = transactions.filter(col("transactionAmount") > 500)

    // Output flagged transactions to console
//    val query = flaggedTransactions.writeStream
//      .outputMode("append")
//      .format("console")
//      .trigger(Trigger.ProcessingTime("10 seconds"))
//      .start()




    /*val query = flaggedTransactions.writeStream
      .outputMode("append")
      .format("csv") // You can use "csv" or other formats if needed
      .option("header", "true")
      .option("path", "src/main/scala/com/its/youtube/chapter2/output/fraud_data") // Replace with your local folder path
      .option("checkpointLocation", "src/main/scala/com/its/youtube/chapter2/output/checkpoint") // Replace with your checkpoint folder path
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()*/

    val query = flaggedTransactions.writeStream
      .outputMode("append")
      .format("parquet") // You can use "csv" or other formats if needed
      .option("header", "true")
      .option("path", "src/main/scala/com/its/youtube/chapter2/output/fraud_data") // Replace with your local folder path
      .option("checkpointLocation", "src/main/scala/com/its/youtube/chapter2/output/checkpoint") // Replace with your checkpoint folder path
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()




    query.awaitTermination()
  }
}
