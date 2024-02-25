package com.its.esales.testCodes

import org.apache.spark.sql.{SaveMode, SparkSession}

import java.sql.{Connection, DriverManager, Statement}
import java.util.Properties
import scala.util.{Failure, Success, Try}
import org.apache.spark.sql.functions._

object DatawarehouseLoad extends App{

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


   val props = new Properties()
     props.setProperty("user", "admin")
     props.setProperty("password", "Admin12345678")

     val initialRecords = spark.read.format("parquet")
       .option("inferSchema", "true")
       .load("s3a://datalake-youtube-itstreamer/ecart/ecart/customer/L*.parquet")

     initialRecords
       .select( "custid", "custname", "custadd")
       .write
       .mode(SaveMode.Append)
       .jdbc("jdbc:redshift://test-workgroup.590183830322.us-east-1.redshift-serverless.amazonaws.com:5439/dev", "ecart_tgt.customer", props)

     val cdcRecords = spark.read.format("parquet")
       .option("inferSchema", "true")
       .load("s3a://datalake-youtube-itstreamer/ecart/ecart/customer/2024/01/05/12/2*.parquet")

     val totalRecords = cdcRecords
     val latestCustCodes = totalRecords.groupBy("CUSTID").agg(max("TRANSACT_ID").alias("TRANSACT_ID"))

   val latestRecords = totalRecords.join(latestCustCodes, Seq("CUSTID","TRANSACT_ID"))
   val finalDf = latestRecords.select("Op", "custid", "custname", "custadd")

   finalDf
     .write
     .mode(SaveMode.Append)
     .jdbc("jdbc:redshift://test-workgroup.590183830322.us-east-1.redshift-serverless.amazonaws.com:5439/dev", "cdc.cd_customer", props)


}
