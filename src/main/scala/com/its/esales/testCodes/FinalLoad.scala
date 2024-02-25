package com.its.esales.testCodes

import com.amazonaws.auth.BasicAWSCredentials

import java.sql.{Connection, DriverManager, Statement}
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials, DefaultAWSCredentialsProviderChain}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.document.{DynamoDB, Item, Table}
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec
import com.amazonaws.services.dynamodbv2.model.{AttributeValue, QueryRequest}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.services.s3.model.GetObjectRequest
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.functions._

import scala.collection.JavaConverters.{asScalaBufferConverter, mapAsJavaMapConverter}

object FinalLoad extends App{

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples3.com")
    .getOrCreate()

  // Set log level to ERROR
  spark.sparkContext.setLogLevel("ERROR")

  // Set S3A configuration properties
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "AKIAQQFHLXYNY2FGF7U7")
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "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")
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")


  // AWS credentials
  val awsCredentials = new BasicAWSCredentials("AKIAYS2NSSMZDMCFVDV7", "AHAXqOcOOTdUQ1qrZxN6AUaYdqbwFU/purHNllih")

  // AWS region
  val region = Regions.US_EAST_1 // Replace with your desired AWS region

  // Create a DynamoDB client
  val dynamoDBClient = AmazonDynamoDBClientBuilder.standard()
    .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
    .withRegion(region)
    .build()

  // DynamoDB table information
  val tableName = "audit_ecart_tables"
  val src_table = "ecart_customer"
  val dynamoDB = new DynamoDB(dynamoDBClient)

  // Perform a query to check if the partition key value exists
  val queryRequest = new QueryRequest()
    .withTableName(tableName)
    .withKeyConditionExpression("schemaname_tablename = :v_partition_key")
    .withExpressionAttributeValues(Map(":v_partition_key" -> new AttributeValue(src_table)).asJava)

  val queryResult = dynamoDBClient.query(queryRequest)

  // DynamoDB table

  val table: Table = dynamoDB.getTable(tableName)

    val putItemSpec = new PutItemSpec()
      .withItem(new Item()
        .withPrimaryKey("schemaname_tablename", src_table)
        .withString("lastReadTransactId", "-1"))

    // Do Initial Load of all L*.parquet
    // Truncate table (just for safe side and for future reference)






  // Check if the item exists
  if (queryResult.getCount == 0) {
    // This table is not present in audit files.It has to be craeted
    println(s"Item with partition key $src_table not found.It will be created with value -1.")


    val putItemSpec = new PutItemSpec()
      .withItem(new Item()
        .withPrimaryKey("schemaname_tablename", src_table)
        // Add other attributes as needed
        .withString("lastReadTransactId", "-1"))
    /// Do Initial Load of all L*.parquet
    // Truncate table (just for safe side and for future reference)

    table.putItem(putItemSpec)
    println(s"Item with partition key $src_table created. with value -1")

    val jdbcUrl = "jdbc:redshift://test-workgroup.590183830322.us-east-1.redshift-serverless.amazonaws.com:5439/dev"
    val targettableName = "ecart_tgt.customer"

    val props = new java.util.Properties()
    props.setProperty("user", "admin")
    props.setProperty("password", "Admin12345678")

    val connection = DriverManager.getConnection(jdbcUrl, props)

    try {
      // Truncate statement
      val truncateQuery = s"TRUNCATE TABLE $targettableName"

      // Execute the truncate query
      val statement = connection.createStatement()
      statement.executeUpdate(truncateQuery)
      println("Table truncated successfully.")
    } catch {
      case ex: Exception => {
        println(s"Error truncating table: ${ex.getMessage}")
      }
    } finally {
      // Close the connection
      //connection.close()
    }

    //Now do initial Load of all data starting with LOAD*.parquet

    val initialRecords = spark.read.format("parquet")
      .option("inferSchema", "true")
      .load("s3a://datalake-youtube-itstreamer/ecart/ecart/customer/L*.parquet")

    initialRecords
      .select("custid", "custname", "custadd")
      .write
      .mode(SaveMode.Append)
      .jdbc("jdbc:redshift://test-workgroup.590183830322.us-east-1.redshift-serverless.amazonaws.com:5439/dev", "ecart_tgt.customer", props)


    //Update your dynamo db document with "0"
    val putItemSpecAfterInitialLoad = new PutItemSpec()
      .withItem(new Item()
        .withPrimaryKey("schemaname_tablename", src_table)
        // Add other attributes as needed
        .withString("lastReadTransactId", "0"))
    /// Do Initial Load of all L*.parquet
    // Truncate table (just for safe side and for future reference)

    table.putItem(putItemSpecAfterInitialLoad)

    println(s"Item with partition key $src_table created. with value 0")


    val cdcRecords = spark.read.format("parquet")
      .option("inferSchema", "true")
      .load("s3a://datalake-youtube-itstreamer/ecart/ecart/customer/*/*/*/*/2*.parquet")

    val totalRecords = cdcRecords
    val latestCustCodes = totalRecords.groupBy("CUSTID").agg(max("TRANSACT_ID").alias("TRANSACT_ID"))

    val latestRecords = totalRecords.join(latestCustCodes, Seq("CUSTID", "TRANSACT_ID"))
    val finalDf = latestRecords.select("Op", "custid", "custname", "custadd")


    try {
      val cdctableName = "cdc.cd_customer"
      // Truncate statement
      val truncateQuery = s"TRUNCATE TABLE $cdctableName"

      // Execute the truncate query
      val statement = connection.createStatement()
      statement.executeUpdate(truncateQuery)
      println("Table truncated successfully.")
    } catch {
      case ex: Exception => {
        println(s"Error truncating table: ${ex.getMessage}")
      }
    } finally {
      // Close the connection
      connection.close()
    }


    finalDf
      .write
      .mode(SaveMode.Append)
      .jdbc("jdbc:redshift://test-workgroup.590183830322.us-east-1.redshift-serverless.amazonaws.com:5439/dev", "cdc.cd_customer", props)
    val maxTransactId: String = latestCustCodes.agg(max("TRANSACT_ID").alias("MAX_TRANSACT_ID")).collect()(0).getAs[String]("MAX_TRANSACT_ID")

    // Now, maxTransactId contains the maximum TRANSACT_ID as a Long variable
    println(s"The maximum TRANSACT_ID is: $maxTransactId")

    val putItemSpecAfterCDCload = new PutItemSpec()
      .withItem(new Item()
        .withPrimaryKey("schemaname_tablename", src_table)
        // Add other attributes as needed
        .withString("lastReadTransactId", maxTransactId))
    /// Do Initial Load of all L*.parquet
    // Truncate table (just for safe side and for future reference)

    table.putItem(putItemSpecAfterCDCload)
    println(s"Item with partition key $src_table created. with value $maxTransactId")


  } else {
    // Item exists, print the results

    val props = new java.util.Properties()
    props.setProperty("user", "admin")
    props.setProperty("password", "Admin12345678")

    println(s"Item with partition key $src_table already exists.")
    val awsRegion = Regions.US_EAST_1

    import java.time.{LocalDateTime, ZoneOffset}
    import java.time.format.DateTimeFormatter

    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy, MM, dd, HH")

    val currentDateTime: LocalDateTime = LocalDateTime.now(ZoneOffset.UTC)

    // Format the LocalDateTime
    val formattedDate: String = currentDateTime.format(formatter)

    println(formattedDate)

    val currentYear = currentDateTime.getYear
    val currentMonth = currentDateTime.format(DateTimeFormatter.ofPattern("MM"))
    val currentDate = currentDateTime.format(DateTimeFormatter.ofPattern("dd"))
    val currentHour = currentDateTime.format(DateTimeFormatter.ofPattern("HH"))

    val lastHourDateTime = currentDateTime.minusHours(1)
    val lastHourYear = lastHourDateTime.getYear
    val lastHourMonth = lastHourDateTime.format(DateTimeFormatter.ofPattern("MM"))
    val lastHourDate = lastHourDateTime.format(DateTimeFormatter.ofPattern("dd"))
    val lastHour = lastHourDateTime.format(DateTimeFormatter.ofPattern("HH"))

      println(currentYear, currentMonth, currentDate, currentHour)
      println(lastHourYear, lastHourMonth, lastHourDate, lastHour)

    val bucketName = "datalake-youtube-itstreamer"

    val baseDirectory = "ecart/ecart/customer"

    val filePathPattern1 = s"s3a://$bucketName/$baseDirectory/$lastHourYear/$lastHourMonth/$lastHourDate/$lastHour/2*.parquet"
    val filePathPattern2 = s"s3a://$bucketName/$baseDirectory/$currentYear/$currentMonth/$currentDate/$currentHour/2*.parquet"





    val s3ClientBuilder = AmazonS3ClientBuilder.standard()
      .withCredentials(new DefaultAWSCredentialsProviderChain())
      .withRegion(awsRegion)


    // Build the AmazonS3 client
    val s3Client: AmazonS3 = s3ClientBuilder.build()



    val objectListingCurrentHour = s3Client.listObjects("datalake-youtube-itstreamer", s"$baseDirectory/$currentYear/$currentMonth/$currentDate/$currentHour")
    val objectListingPreviousHour = s3Client.listObjects("datalake-youtube-itstreamer", s"$baseDirectory/$lastHourYear/$lastHourMonth/$lastHourDate/$lastHour")
    val fileExistsCurrentHour = objectListingCurrentHour.getObjectSummaries.size() > 0
    val fileExistsPreviousHour = objectListingPreviousHour.getObjectSummaries.size() > 0

    val lasttransactIdValue: String = queryResult.getItems.asScala.headOption
      .flatMap(item => Option(item.get("lastReadTransactId")).map(_.getS))
      .getOrElse {
        // Default value or error handling if needed
        throw new NoSuchElementException("lastReadTransactId not found in DynamoDB query result")
      }
   println(lasttransactIdValue)

    val combinedDF: DataFrame = if (fileExistsCurrentHour && fileExistsPreviousHour) {
      spark.read
        .format("parquet")
        .option("inferSchema", "true")
        .load(filePathPattern1, filePathPattern2)
        .filter(col("TRANSACT_ID") > lasttransactIdValue)


    } else if (fileExistsCurrentHour) {
      spark.read
        .format("parquet")
        .option("inferSchema", "true")
        .load(filePathPattern2)
        .filter(col("TRANSACT_ID") > lasttransactIdValue)
    }
    else if (fileExistsPreviousHour) {
      spark.read
        .format("parquet")
        .option("inferSchema", "true")
        .load(filePathPattern1)
        .filter(col("TRANSACT_ID") > lasttransactIdValue)
    }
    else {
      spark.createDataFrame(Seq.empty[(String, Int)])
    }


    println(combinedDF.count())

    if (combinedDF.isEmpty) {
      dynamoDBClient.shutdown()
    } else {


      val totalRecords = combinedDF
      val latestCustCodes = totalRecords.groupBy("CUSTID").agg(max("TRANSACT_ID").alias("TRANSACT_ID"))

      val latestRecords = totalRecords.join(latestCustCodes, Seq("CUSTID", "TRANSACT_ID"))
      val finalDf = latestRecords.select("Op", "custid", "custname", "custadd")

      val jdbcUrl = "jdbc:redshift://test-workgroup.590183830322.us-east-1.redshift-serverless.amazonaws.com:5439/dev"
      val props = new java.util.Properties()
      props.setProperty("user", "admin")
      props.setProperty("password", "Admin12345678")

      val connection = DriverManager.getConnection(jdbcUrl, props)
      try {
        val cdctableName = "cdc.cd_customer"
        // Truncate statement
        val truncateQuery = s"TRUNCATE TABLE $cdctableName"

        // Execute the truncate query
        val statement = connection.createStatement()
        statement.executeUpdate(truncateQuery)
        println("Table truncated successfully.")
      } catch {
        case ex: Exception => {
          println(s"Error truncating table: ${ex.getMessage}")
        }
      } finally {
        // Close the connection
        connection.close()
      }
      finalDf
        .write
        .mode(SaveMode.Append)
        .jdbc("jdbc:redshift://test-workgroup.590183830322.us-east-1.redshift-serverless.amazonaws.com:5439/dev", "cdc.cd_customer", props)
      val maxTransactId: String = latestCustCodes.agg(max("TRANSACT_ID").alias("MAX_TRANSACT_ID")).collect()(0).getAs[String]("MAX_TRANSACT_ID")
      // Now, maxTransactId contains the maximum TRANSACT_ID as a Long variable
      println(s"The maximum TRANSACT_ID is: $maxTransactId")

      val putItemSpecAfterCDCload2 = new PutItemSpec()
        .withItem(new Item()
          .withPrimaryKey("schemaname_tablename", src_table)
          // Add other attributes as needed
          .withString("lastReadTransactId", maxTransactId))
      /// Do Initial Load of all L*.parquet
      // Truncate table (just for safe side and for future reference)

      table.putItem(putItemSpecAfterCDCload2)
      println(s"Item with partition key $src_table created. with value $maxTransactId")
      dynamoDBClient.shutdown()


    }

  }




  // Close the DynamoDB client
  dynamoDBClient.shutdown()

}
