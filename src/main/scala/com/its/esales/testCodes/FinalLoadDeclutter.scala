package com.its.esales.testCodes

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.Regions
import com.its.esales.framework.jobs.controllers.BaseController
import com.its.esales.framework.util.calendar.DateTimeInfo
import com.its.esales.framework.util.dynamodb.DynamoDBHandler
import com.its.esales.framework.util.jdbc.JDBCHandler
import com.its.esales.framework.util.s3.s3Handler
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.functions._

import java.time.{LocalDateTime, ZoneOffset}

object FinalLoadDeclutter extends App with BaseController {

  val awsCredentials = new BasicAWSCredentials("AKIAYS2NSSMZDMCFVDV7", "AHAXqOcOOTdUQ1qrZxN6AUaYdqbwFU/purHNllih")
  val region = Regions.US_EAST_1
  val dynamoDocName = "audit_ecart_tables"
  val hashKeyColumn = "schemaname_tablename"
  val hashKey = "ecart_customer"
  val jdbcUrl = "jdbc:redshift://test-workgroup.590183830322.us-east-1.redshift-serverless.amazonaws.com:5439/dev"
  val jdbcUser = "admin"
  val jdbcPassword = "Admin12345678"
  val targetTableName = "ecart_tgt.customer"
  val deltaTableName = "cdc.cd_customer"
  val s3FolderPath = "ecart/ecart/customer"
  val s3FilePathFullLoad = "ecart/ecart/customer/L*.parquet"
  val s3FilePathDeltaLoad = "ecart/ecart/customer/*/*/*/*/2*.parquet"
  val bucketName = "datalake-youtube-itstreamer"


    val dynamoDBHandler = new DynamoDBHandler(dynamoDocName, hashKeyColumn, awsCredentials, region)
    val jdbcHandler = new JDBCHandler(jdbcUrl, jdbcUser, jdbcPassword)
    val s3handler = new s3Handler(
        bucketName = bucketName,
        awsCredentials =awsCredentials,
        awsRegion = region, // Specify the file pattern, can be an empty string
      )

  if (dynamoDBHandler.countDynamoDocRecords(hashKey) ==0 || dynamoDBHandler.extractRecordForHashKey(hashKey,"lastReadTransactId") =="-1") {

    dynamoDBHandler.insertRecordDynamoDocument(hashKey,"lastReadTransactId", "-1")
    println(s"Item with partition key $hashKey created. with value -1")

    val initialRecords = s3handler.readParquetFile(s3FilePathFullLoad).select("custid", "custname", "custadd")
    jdbcHandler.overwriteDataFrameToJDBC(initialRecords,targetTableName)
    dynamoDBHandler.insertRecordDynamoDocument(hashKey,"lastReadTransactId", "0")

    val completeCDCRecords = s3handler.readParquetFile(s3FilePathDeltaLoad)
    val latestCustCodes = completeCDCRecords.groupBy("CUSTID").agg(max("TRANSACT_ID").alias("TRANSACT_ID"))

    val latestRecords = completeCDCRecords.join(latestCustCodes, Seq("CUSTID", "TRANSACT_ID"))
    val finalDf = latestRecords.select("Op", "custid", "custname", "custadd")
    jdbcHandler.overwriteDataFrameToJDBC(finalDf,deltaTableName)
    val maxTransactId: String = latestCustCodes.agg(max("TRANSACT_ID").alias("MAX_TRANSACT_ID")).collect()(0).getAs[String]("MAX_TRANSACT_ID")
    println(s"The maximum TRANSACT_ID is: $maxTransactId")

    jdbcHandler.executeQuery("INSERT INTO ecart_tgt.CUSTOMER (CUSTID, CUSTNAME, CUSTADD)\nSELECT SRC.CUSTID, SRC.CUSTNAME, SRC.CUSTADD\nFROM CDC.CD_CUSTOMER SRC\nWHERE SRC.OP = 'I'  ;")
    jdbcHandler.executeQuery("INSERT INTO ecart_tgt.CUSTOMER (CUSTID, CUSTNAME, CUSTADD)\nSELECT SRC.CUSTID, SRC.CUSTNAME, SRC.CUSTADD\nFROM CDC.CD_CUSTOMER SRC\nWHERE SRC.OP = 'U'\n  AND NOT EXISTS (\n    SELECT 1\n    FROM ecart_tgt.CUSTOMER\n    WHERE ecart_tgt.CUSTOMER.CUSTID = SRC.CUSTID\n  );")
    jdbcHandler.executeQuery("UPDATE ecart_tgt.CUSTOMER\nSET\n  CUSTNAME = SRC.CUSTNAME,\n  CUSTADD = SRC.CUSTADD\nFROM CDC.CD_CUSTOMER SRC\nWHERE ecart_tgt.CUSTOMER.CUSTID = SRC.CUSTID\n  AND SRC.OP = 'U';")
    jdbcHandler.executeQuery("DELETE FROM ecart_tgt.CUSTOMER\nUSING CDC.CD_CUSTOMER SRC\nWHERE ecart_tgt.CUSTOMER.CUSTID = SRC.CUSTID\n  AND SRC.OP = 'D';")
    dynamoDBHandler.insertRecordDynamoDocument(hashKey,"lastReadTransactId", maxTransactId)
  }
  else {
    val dateTimeInfo = new DateTimeInfo(LocalDateTime.now(ZoneOffset.UTC))
    val filepath1 = s"$s3FolderPath/${dateTimeInfo.lastHourYear}/${dateTimeInfo.lastHourMonth}/${dateTimeInfo.lastHourDate}/${dateTimeInfo.lastHour}"
    val filepath2 = s"$s3FolderPath/${dateTimeInfo.currentYear}/${dateTimeInfo.currentMonth}/${dateTimeInfo.currentDate}/${dateTimeInfo.currentHour}"

    val s3uriPath1 = s"s3a://$bucketName/$filepath1/2*.parquet"
    val s3uriPath2 = s"s3a://$bucketName/$filepath2/2*.parquet"

    val fileExistsPreviousHour = s3handler.checkFileExists(filepath1)
    val fileExistsCurrentHour = s3handler.checkFileExists(filepath2)


    val lasttransactIdValue: String = dynamoDBHandler.extractRecordForHashKey(hashKey,"lastReadTransactId")

    val combinedDF: DataFrame = if (fileExistsCurrentHour && fileExistsPreviousHour) {
      s3handler.readMultipleParquetFiles(filepath1,filepath2).filter(col("TRANSACT_ID") > lasttransactIdValue)

    } else if (fileExistsCurrentHour) {
      s3handler.readParquetFile(filepath2)
        .filter(col("TRANSACT_ID") > lasttransactIdValue)
    }
    else if (fileExistsPreviousHour) {
      s3handler.readParquetFile(filepath1)
        .filter(col("TRANSACT_ID") > lasttransactIdValue)
    }
    else {
      spark.createDataFrame(Seq.empty[(String, Int)])
    }

    if (combinedDF.isEmpty){

    }
    else{
      combinedDF.show()
      val latestCustCodes = combinedDF.groupBy("CUSTID").agg(max("TRANSACT_ID").alias("TRANSACT_ID"))
      val latestRecords = combinedDF.join(latestCustCodes, Seq("CUSTID", "TRANSACT_ID"))
      val finalDf = latestRecords.select("Op", "custid", "custname", "custadd")
      finalDf.show()
      jdbcHandler.overwriteDataFrameToJDBC(finalDf,deltaTableName)
      val maxTransactId: String = latestCustCodes.agg(max("TRANSACT_ID").alias("MAX_TRANSACT_ID")).collect()(0).getAs[String]("MAX_TRANSACT_ID")
      println(s"The maximum TRANSACT_ID is: $maxTransactId")

      jdbcHandler.executeQuery("INSERT INTO ecart_tgt.CUSTOMER (CUSTID, CUSTNAME, CUSTADD)\nSELECT SRC.CUSTID, SRC.CUSTNAME, SRC.CUSTADD\nFROM CDC.CD_CUSTOMER SRC\nWHERE SRC.OP = 'I'  ;")
      jdbcHandler.executeQuery("INSERT INTO ecart_tgt.CUSTOMER (CUSTID, CUSTNAME, CUSTADD)\nSELECT SRC.CUSTID, SRC.CUSTNAME, SRC.CUSTADD\nFROM CDC.CD_CUSTOMER SRC\nWHERE SRC.OP = 'U'\n  AND NOT EXISTS (\n    SELECT 1\n    FROM ecart_tgt.CUSTOMER\n    WHERE ecart_tgt.CUSTOMER.CUSTID = SRC.CUSTID\n  );")
      jdbcHandler.executeQuery("UPDATE ecart_tgt.CUSTOMER\nSET\n  CUSTNAME = SRC.CUSTNAME,\n  CUSTADD = SRC.CUSTADD\nFROM CDC.CD_CUSTOMER SRC\nWHERE ecart_tgt.CUSTOMER.CUSTID = SRC.CUSTID\n  AND SRC.OP = 'U';")
      jdbcHandler.executeQuery("DELETE FROM ecart_tgt.CUSTOMER\nUSING CDC.CD_CUSTOMER SRC\nWHERE ecart_tgt.CUSTOMER.CUSTID = SRC.CUSTID\n  AND SRC.OP = 'D';")
      dynamoDBHandler.insertRecordDynamoDocument(hashKey,"lastReadTransactId", maxTransactId)

    }

    }
  }
