package com.its.esales.framework.util.s3

import com.amazonaws.auth.{BasicAWSCredentials, DefaultAWSCredentialsProviderChain}
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import org.apache.spark.sql.{DataFrame, SparkSession}

class s3Handler (bucketName: String,  awsCredentials: BasicAWSCredentials, awsRegion: Regions) {

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]").appName("SparkByExamples3.com")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", awsCredentials.getAWSAccessKeyId)
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key",  awsCredentials.getAWSSecretKey)
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")

  private val s3ClientBuilder = AmazonS3ClientBuilder.standard()
    .withCredentials(new DefaultAWSCredentialsProviderChain())
    .withRegion(awsRegion)
  private val s3Client: AmazonS3 = s3ClientBuilder.build()

  def checkFileExists(filePath : String): Boolean = {
    val objectListing = s3Client.listObjects(bucketName, s"$filePath")
    objectListing.getObjectSummaries.size() > 0
  }

  def readMultipleParquetFiles(filePaths: String*): DataFrame = {
    val filePathPatterns = filePaths.map(value => s"s3a://$bucketName/$value")

      spark.read
        .format("parquet")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(filePathPatterns: _*)

  }

  def readParquetFile(pathInsideBucket: String): DataFrame = {

    spark.read
      .format("parquet")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(s"s3a://$bucketName/$pathInsideBucket")

  }

  def readCSVFile(pathInsideBucket: String): DataFrame = {

    spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("multiline", "true")
      .load(s"s3a://$bucketName/$pathInsideBucket")

  }

  def readMultipleCSVFiles(pathInsideBucket: String*): DataFrame = {
    val filePathPatterns = pathInsideBucket.map(value => s"s3a://$bucketName/$pathInsideBucket")

    spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("multiline", "true")
      .load(filePathPatterns: _*)

  }


}


