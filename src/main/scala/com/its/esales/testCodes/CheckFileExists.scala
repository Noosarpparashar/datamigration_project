package com.its.esales.testCodes

import com.amazonaws.auth.{DefaultAWSCredentialsProviderChain}
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._


object CheckFileExists extends App{

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]").appName("SparkByExamples3.com")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  spark.sparkContext
    .hadoopConfiguration.set("fs.s3a.access.key", "AKIAYS2NSSMZDMCFVDV7")
  spark.sparkContext
    .hadoopConfiguration.set("fs.s3a.secret.key", "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")
  spark.sparkContext
    .hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")



  val awsRegion = Regions.US_EAST_1

  private val s3ClientBuilder = AmazonS3ClientBuilder.standard()
    .withCredentials(new DefaultAWSCredentialsProviderChain())
    .withRegion(awsRegion)


  // Build the AmazonS3 client
  private val s3Client: AmazonS3 = s3ClientBuilder.build()

  val bucketName = "datalake-youtube-itstreamer"
  val baseDirectory = "ecart/ecart/customer"
  val value1 = "2024/01/05/13"
  val value2 = "2024/01/05/13"
  val filePathPattern1 = s"s3a://$bucketName/$baseDirectory/$value1/2*.parquet"
  val filePathPattern2 = s"s3a://$bucketName/$baseDirectory/$value2/2*.parquet"


  val objectListingCurrentHour = s3Client.listObjects("datalake-youtube-itstreamer", s"$baseDirectory/$value2")
  val objectListingPreviousHour = s3Client.listObjects("datalake-youtube-itstreamer", s"$baseDirectory/$value1")
  val fileExistsCurrentHour = objectListingCurrentHour.getObjectSummaries.size() > 0
  val fileExistsPreviousHour = objectListingPreviousHour.getObjectSummaries.size() > 0

  val combinedDF: DataFrame = if (fileExistsCurrentHour && fileExistsPreviousHour) {
    spark.read
      .format("parquet")
      .option("inferSchema", "true")
      .load(filePathPattern1, filePathPattern2)


  } else if (fileExistsCurrentHour) {
    spark.read
      .format("parquet")
      .option("inferSchema", "true")
      .load(filePathPattern2)
  }
  else if (fileExistsPreviousHour) {
    spark.read
      .format("parquet")
      .option("inferSchema", "true")
      .load(filePathPattern1)
  }
  else {
    spark.createDataFrame(Seq.empty[(String, Int)])
  }

  println(combinedDF.count())


}
