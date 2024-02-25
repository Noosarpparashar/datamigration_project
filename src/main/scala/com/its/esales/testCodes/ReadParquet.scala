package com.its.esales.testCodes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ReadParquet extends App {

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

//    val initialRecordsParquet = spark.read.format("parquet")
//      .option("inferSchema", "true")
//      //.option("multiline", "true")
//      .load("s3a://datalake-youtube-itstreamer/ecart/ecart/customer/L*.parquet")
//
//    initialRecordsParquet.filter(col("custid")==="C04455870").show()

    val cdcRecordsParquet = spark.read.format("parquet")
      .option("inferSchema", "true")
      .option("multiline", "true")
      .load("s3a://datalake-youtube-itstreamer/ecart/ecart/customer/2024/*/*/*/2*.parquet")

//
//    cdcRecordsParquet.filter(col("Op")==="I").show(false)
//    cdcRecordsParquet.filter(col("Op")==="U").show(false)
//    cdcRecordsParquet.filter(col("Op")==="D").show(false)
//
  cdcRecordsParquet.filter(col("custid")==="C04455870").show()


}
