package com.its.esales.batch.common

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.Regions
import com.its.esales.framework.exception.InvalidConfigurationException
import com.its.esales.framework.jobs.Job
import com.its.esales.framework.jobs.controllers.BaseController
import com.its.esales.framework.traits.Logging
import com.its.esales.framework.util.dynamodb.DynamoDBHandler
import com.its.esales.framework.util.jdbc.JDBCHandler
import com.its.esales.framework.util.s3.s3Handler
import com.its.esales.framework.util.encryption.SecretManager
import com.softwaremill.macwire._
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.max
class BatchETL extends BaseController with Logging with Job  with Serializable  {

  lazy val s3FolderPath: String = ""
  lazy val bucketName: String = ""
  lazy val awsCredentials: BasicAWSCredentials =new BasicAWSCredentials("", "")
  lazy val region: Option[Regions] = None
  lazy val dynamoDocName: Option[String] = None
  lazy val hashKey: Option[String] = None
  lazy val jdbcUrl: String = ""
  lazy val jdbcUser: String = ""
  lazy val jdbcPassword: String = ""
  lazy val targetTableName: String = ""
  lazy val deltaTableName: String = ""
  lazy val hashKeyColumn: Option[String] = None


  protected lazy val awsCreds: BasicAWSCredentials = awsCredentials match {
    case awsCreds: BasicAWSCredentials => awsCreds
    case _ =>
      val errorMessage =
        """Invalid configuration: "AWSCredentials" value must be overridden. """
      logger.error(errorMessage)
      throw InvalidConfigurationException(errorMessage)
  }

  lazy val dynamoDBHandler = new DynamoDBHandler(dynamoDocName.get, hashKeyColumn.get, awsCredentials, region.get)
  lazy val jdbcHandler = new JDBCHandler(jdbcUrl, jdbcUser, jdbcPassword)
  lazy val s3Handler = new s3Handler(
    bucketName = bucketName,
    awsCredentials = awsCreds,
    awsRegion = region.get,
  )

  def getPassword(key: String): String = {
    // Instantiate SecretManager
    val secretManager = new SecretManager()
    println( secretManager.getAndDecryptSecret("prod/ecart_migration/jdbcDB",key))
    secretManager.getAndDecryptSecret("prod/ecart_migration/jdbcDB",key)
  }

  def getConfig(key: String): String = {
    // Instantiate SecretManager
    val secretManager = new SecretManager()
    println(secretManager.retrieveFromSecrets("prod/ecart_migration/jdbcDB",key))
    secretManager.retrieveFromSecrets("prod/ecart_migration/jdbcDB",key)
  }


  def extractParquetFromS3(filePath: String): DataFrame = {
    // Use s3Handler to read from S3
    s3Handler.readParquetFile(filePath)
  }

  def extractCSVFromS3(filePath: String): DataFrame = {
    // Use s3Handler to read from S3
    s3Handler.readCSVFile(filePath)
  }

  def extractMultipleParquetFromS3(filePath: String): DataFrame = {
    // Use s3Handler to read from S3
    s3Handler.readParquetFile(filePath)
  }

  def extractTableFromJDBC(tableName:String):DataFrame = {
    jdbcHandler.extractFromJDBC(tableName)
  }

  def extractQueryFromJDBC(query: String,numPartitions:Int = 10, fetchsize:Int = 10000): DataFrame = {
    jdbcHandler.extractQueryFromJDBC(query)
  }



  def transformBulkLoad(inputDF: DataFrame): DataFrame = {
    /*
    * Overide this function only if you think your transformation logic is different for first time bulk load and delta load
    * Or your transformation logic involves operation in column "Op". Since this column is not present in First time load, it can create issues
    * Otheriwse just override your transformation logic in transform function only
    * */
    transform(inputDF)
  }

  def transform(inputDF: DataFrame): DataFrame = {
    // Your transformations here
    inputDF
  }


  def loadParquetToS3(dataFrame: DataFrame, outputPath: String): Unit = {
    // Use s3Handler to write to S3
    dataFrame.write.mode(SaveMode.Overwrite).parquet(outputPath)
  }

  def reloadToJDBC(dataFrame: DataFrame, tableName: String): Unit = {
    logger.info(s"Overwriting dataframe to $tableName")
    jdbcHandler.overwriteDataFrameToJDBC(dataFrame, tableName)
  }

  def loadToJDBC(dataFrame: DataFrame, tableName: String): Unit = {
    logger.info(s"Appending dataframe to $tableName")
    jdbcHandler.appendDataFrameToJDBC(dataFrame, tableName)
  }





}
