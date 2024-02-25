package com.its.esales.batch

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.Regions
import com.its.esales.framework.jobs.controllers.BaseController
import com.its.esales.framework.util.Redshift.RedshiftHandler
import com.its.esales.framework.util.calendar.DateTimeInfo
import com.its.esales.framework.util.dynamodb.DynamoDBHandler
import com.its.esales.framework.util.jdbc.JDBCHandler
import com.its.esales.framework.util.s3.s3Handler

import java.time.{LocalDateTime, ZoneOffset}

object TestClasses extends App with BaseController {


  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  /*
   This is to test DynaomoDb Functions
  * */


/*
  val awsCredentials = new BasicAWSCredentials("AKIAYS2NSSMZDMCFVDV7", "AHAXqOcOOTdUQ1qrZxN6AUaYdqbwFU/purHNllih")
  val region = Regions.US_EAST_1
  val dynamoDocName = "audit_ecart_tables"
  val hashKeyColumn = "schemaname_tablename"
  try {
    val dynamoDBHandler = new DynamoDBHandler(dynamoDocName,hashKeyColumn, awsCredentials, region)
    val hashKey = "ecart_customer"
    dynamoDBHandler.insertRecordDynamoDocument(hashKeyColumn, "lastReadTransactId","2022222")


    val queryResult = dynamoDBHandler.performQueryToCheckPartitionKey(hashKey)
    println(queryResult)

    val queryCount = dynamoDBHandler.countDynamoDocRecords(hashKey)
    println(queryCount)

    val extractItem = dynamoDBHandler.extractRecordForHashKey(hashKey,"lastReadTransactId")
    println(extractItem)

  } finally {
    // The dynamoDBHandler.close() will be called automatically when exiting the try block
  }

 */

  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  // This is to connect JDBC
/*
  val jdbcUrl = "jdbc:redshift://test-workgroup.590183830322.us-east-1.redshift-serverless.amazonaws.com:5439/dev"
  val jdbcUser = "admin"
  val jdbcPassword = "Admin12345678"

  // Example using JDBCHandler
  val jdbcHandler = new JDBCHandler(jdbcUrl, jdbcUser, jdbcPassword)
  try {
    // Example 1: Execute a generic query
    val genericQuery = s"INSERT INTO ecart_tgt.customer (CUSTID, CUSTNAME, CUSTADD) VALUES ('12345', 'prasoon', 'Bangalore1');"
    jdbcHandler.executeQuery(genericQuery)

    // Example 2: Execute a PreparedStatement
    val tableName = "ecart_tgt.customer"
    val preparedStatement = s"INSERT INTO $tableName (CUSTID, CUSTNAME, CUSTADD) VALUES (?, ?, ?)"
    val parameters = Seq("12346", "prasoon", "Bangalore2")
    jdbcHandler.executePreparedStatement(preparedStatement, parameters)
  } finally {
    jdbcHandler.close()
  }

   */
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


  ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  /*
This is to connect with Redshift
 */
  /*
  val jdbcUrl = "jdbc:redshift://test-workgroup.590183830322.us-east-1.redshift-serverless.amazonaws.com:5439/dev"
  val jdbcUser = "admin"
  val jdbcPassword = "Admin12345678"
  val redshiftSchema = "ecart_tgt"
  val redshiftHandler = new RedshiftHandler(jdbcUrl, jdbcUser, jdbcPassword, redshiftSchema)

  try {
    val genericQuery = s"UPDATE $redshiftSchema.customer SET CUSTADD = 'Bangalore3' WHERE custid = '12345' ;"
    redshiftHandler.executeQuery(genericQuery)
    val redShiftSpecificQuery = s"UPDATE $redshiftSchema.customer SET CUSTADD = 'Bangalore3' WHERE custid = '1234' ;"
    redshiftHandler.performRedshiftSpecificOperation(redShiftSpecificQuery)

    val redShiftFetchRecords = s"SELECT * from  $redshiftSchema.customer WHERE custid = '1234' ;"
    val records = redshiftHandler.fetchRecordsFromRedshift(redShiftFetchRecords)
    while (records.next()) {
      val column1Value = records.getString("custid")
      val column2Value = records.getString("custname")

      println(s"Column1: $column1Value, Column2: $column2Value")
    }
  }
  catch {
    case ex: Exception =>
      // Rethrow the caught exception
      throw new RuntimeException(s"Error executing query: ${ex.getMessage}", ex)
  }finally {
    redshiftHandler.close()
  }
*/
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////


  ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  /*
* This is to connect with s3
* */


//    val awsCredentials = new BasicAWSCredentials("AKIAYS2NSSMZDMCFVDV7", "AHAXqOcOOTdUQ1qrZxN6AUaYdqbwFU/purHNllih")
//    val region = Regions.US_EAST_1
//
//    val s3handler = new s3Handler(
//      bucketName = "datalake-youtube-itstreamer",
//      awsCredentials =awsCredentials,
//      awsRegion = region, // Specify the file pattern, can be an empty string
//    )
//    val s3CSVPath = "csv-manual"
//    val df = s3handler.readCSVFile(s3CSVPath)
//    df.show()
//
//    val checkFile = s3handler.checkFileExists(s3CSVPath)
//    println(checkFile)
//
//    val combinedfs =s3handler.readMultipleParquetFiles("customerinfo/20231229_181257/*.parquet", "customerinfo/20240102_123846//*.parquet")
//    println(combinedfs.count())


  ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////


  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  /*
  * This is to handle date time*/

/*
  val dateTimeInfo = new DateTimeInfo(LocalDateTime.now(ZoneOffset.UTC))
  println(s"Formatted Date: ${dateTimeInfo.formattedDate}")
  println(s"Current Year: ${dateTimeInfo.currentYear}")
  println(s"Current Month: ${dateTimeInfo.currentMonth}")
  println(s"Current Date: ${dateTimeInfo.currentDate}")
  println(s"Current Hour: ${dateTimeInfo.currentHour}")


  // Print information for the last hour
  println(s"Last Hour Year: ${dateTimeInfo.lastHourYear}")
  println(s"Last Hour Month: ${dateTimeInfo.lastHourMonth}")
  println(s"Last Hour Date: ${dateTimeInfo.lastHourDate}")
  println(s"Last Hour: ${dateTimeInfo.lastHour}")

   println(s"Yesterday Date ${dateTimeInfo.yesterdayDate}" )
  println(s"Previous Month  ${dateTimeInfo.previousMonth}" )
  println(s" Previous Year${dateTimeInfo.previousYear}" )

  println(s"Previous Month  ${dateTimeInfo.previousMonth}" )
 println(s" Previous Month  Year${dateTimeInfo.previousMonthYear}" )
*/


  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
}