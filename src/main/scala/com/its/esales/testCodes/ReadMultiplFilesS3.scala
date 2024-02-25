package com.its.esales.testCodes

import org.apache.spark.sql.SparkSession

object ReadMultiplFilesS3 extends App{

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



  val value1 = "12"
  val value2 = "13"

  val baseDirectory = "s3a://datalake-youtube-itstreamer/ecart/ecart/customer/2024/01/05"
  val filePathPattern1 = s"$baseDirectory/$value1/2*.parquet"
  val filePathPattern2 = s"$baseDirectory/$value2/2*.parquet"

  val cdcRecords = spark.read
    .format("parquet")
    .option("inferSchema", "true")
    .load(filePathPattern1, filePathPattern2)




  // Specify the file path pattern using the provided values


  // Read CSV files matching the pattern
  cdcRecords.show()
  println(cdcRecords.count())


}
