package com.its.esales.streaming.kafka.consumer

import org.apache.spark.sql.SparkSession
import com.its.esales.framework.jobs.{Job, JobEnum}
import org.apache.spark.sql.functions.{col, from_json, json_tuple, regexp_extract, timestamp_seconds}
import org.apache.spark.sql.types._

import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
class CustomerS3Ingestor(spark: SparkSession) extends Job {


  import spark.implicits._

  val kafkaBootstrapServers = "http://34.125.26.43:9092,http://34.125.26.43:9093,http://34.125.26.43:9094"
  //val kafkaBootstrapServers = "http://localhost:9092,http://localhost:9093,http://localhost:9094"

  val topic = "customerinfo"
  val startingOffsets = "earliest"
  val checkpointLocation = "s3a://datalake-youtube-itstreamer/checkpoint_customerinfo/"
  val path = "s3a://datalake-youtube-itstreamer/customerinfo/"

  // Assuming your JSON has the structure you provided
  val schema = StructType(Seq(
    StructField("CustomerId", StringType),
    StructField("CustomerName", StringType),
    StructField("CustomerAdd", StringType)
  ))

  val dateFormat = "yyyyMMdd_HHmmss"
  private val dateTimeFormatter = DateTimeFormatter.ofPattern(dateFormat)
  println("1111")
  val kafkaDF = spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafkaBootstrapServers) .option("subscribe", topic).load()
  println("2222")
  val jsonParsedDF = kafkaDF
    .selectExpr("CAST(value as STRING) as jsonString")
    .withColumn("jsonString", regexp_extract(col("jsonString"), "\\{.*\\}", 0)) // Extract JSON content
    .select(from_json(col("jsonString"), schema).as("json"))
    .select("json.*")

   // .select(json_tuple(col("value"), "CustomerId", "CustomerName", "CustomerAdd"))
    //.select(from_json(col("cleanedJsonString"), schema).as("json"))
    //.toDF("schema", "payload")

  jsonParsedDF.writeStream
//    .format("console")
//    .option("truncate", false)
    //.outputMode("append")
//    .start()
//    .awaitTermination()
    .format("parquet")
    .outputMode("append")
    .option("checkpointLocation", checkpointLocation)
    .option("path", path + LocalDateTime.now().format(dateTimeFormatter))
    .start()
    .awaitTermination()
  println("3333")


  // Wait for the streaming query to terminate


}
