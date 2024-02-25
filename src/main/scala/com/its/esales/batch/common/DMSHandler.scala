package com.its.esales.batch.common

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, max}
import com.its.esales.framework.exception.InvalidConfigurationException
import com.its.esales.framework.jobs.controllers.BaseController
import com.its.esales.framework.util.calendar.DateTimeInfo

import java.time.{LocalDateTime, ZoneOffset}

class DMSHandler extends BatchETL with BaseController {


  lazy val s3FilePathFullLoad: String = s"$s3FolderPath/L*.parquet"
  lazy val s3FilePathDeltaLoad: String = s"$s3FolderPath/*/*/*/*/2*.parquet"
  lazy val dataLakeColumns: Option[Seq[String]] = None
  lazy val naturalKeyList: Option[Seq[String]] = None
  lazy val targetTableColumns: Option[Seq[String]] = None
  lazy val targetTableNaturalKeysList: Option[Seq[String]] = None
  lazy val targetTableNonNaturalKeys: Seq[String] = targetTableColumns.get.diff(targetTableNaturalKeysList.get)
  lazy val dateTimeInfo = new DateTimeInfo(LocalDateTime.now(ZoneOffset.UTC))

  /*
  * Ensure required columns are overridden
  * */
  protected lazy val dataLakeCols: Seq[String] = dataLakeColumns match {
    case Some(dataLakeCols: Seq[String]) => dataLakeCols
    case None =>
      val errorMessage =
        """Invalid configuration: "DataLake columns must be overridden. """
      logger.error(errorMessage)
      throw InvalidConfigurationException(errorMessage)
  }
  protected lazy val naturalKeys: Seq[String] = naturalKeyList match {
    case Some(naturalKeys: Seq[String]) => naturalKeys
    case None =>
      val errorMessage =
        """Invalid configuration: "Data lake natural keys  must be overridden. """
      logger.error(errorMessage)
      throw InvalidConfigurationException(errorMessage)
  }

  protected lazy val targetTableCols: Seq[String] = targetTableColumns match {
    case Some(targetTableCols: Seq[String]) => targetTableCols
    case None =>
      val errorMessage =
        """Invalid configuration: "Target Table  columns must be overridden. """
      logger.error(errorMessage)
      throw InvalidConfigurationException(errorMessage)
  }

  protected lazy val targetTableNaturalKeys: Seq[String] = targetTableNaturalKeysList match {
    case Some(targetTableNaturalKeys: Seq[String]) => targetTableNaturalKeys
    case None =>
      val errorMessage =
        """Invalid configuration: "Target Table natural keys must be overridden. """
      logger.error(errorMessage)
      throw InvalidConfigurationException(errorMessage)
  }

  def processLoad(deltaLoadType: String = "Hourly"): Unit = {
    if (shouldRunInitialLoad()) {
      processInitialLoad()
    } else {
      processDeltaLoad(deltaLoadType)

    }
  }

  def processDeltaLoad(deltaLoadType: String): Unit = {
    deltaLoadType match {
      case "Hourly" => processHourlyDeltaLoad()
      case "Daily" => processDailyDeltaLoad()
      case "Monthly" => processMonthlyDeltaLoad()
      case "Yearly" => processYearlyDeltaLoad()
      case _ =>
        println(s"Invalid delta load type: $deltaLoadType. Defaulting to Hourly.")
        processHourlyDeltaLoad()
    }
  }


  def processInitialLoad(): Unit = {
      insertOrUpdateDynamoDoc(hashKey.get, "lastReadTransactId", "-1")
      val initialRecords = extractInitialLoadParquet(dataLakeCols)
      val transformedDF = transformBulkLoad(initialRecords)
      reloadToJDBC(transformedDF, targetTableName)
      insertOrUpdateDynamoDoc(hashKey.get, "lastReadTransactId", "0")
      processAllDeltaRecords()
  }

  def processAllDeltaRecords(): Unit = {
      val completeCDCRecords = readDeltaLoadRecords(s3FilePathDeltaLoad)
      val (latestRecords,maxTransactId )= extractLatestRecords(completeCDCRecords,naturalKeys,dataLakeCols, "TRANSACT_ID")
      val transformedDF = transform(latestRecords)
      reloadToJDBC(transformedDF, deltaTableName)
      processCDCRecords()
      insertOrUpdateDynamoDoc(hashKey.get, "lastReadTransactId", maxTransactId)
  }



  def processHourlyDeltaLoad(): Unit = {
    val filepath1 = s"$s3FolderPath/${dateTimeInfo.lastHourYear}/${dateTimeInfo.lastHourMonth}/${dateTimeInfo.lastHourDate}/${dateTimeInfo.lastHour}"
    val filepath2 = s"$s3FolderPath/${dateTimeInfo.currentYear}/${dateTimeInfo.currentMonth}/${dateTimeInfo.currentDate}/${dateTimeInfo.currentHour}"
    val combinedDF = readCurrentAndLastFilePaths(filepath1, filepath2)
    if (combinedDF.isEmpty) {
      logger.info(s"DataFrame is empty. Exiting the code.")
      sys.exit(1)
    }
    else {
      val (latestRecords, maxTransactId) = extractLatestRecords(combinedDF, naturalKeys,dataLakeCols, "TRANSACT_ID")
      val transformedDF = transform(latestRecords)
      reloadToJDBC(transformedDF, deltaTableName)
      processCDCRecords()
      insertOrUpdateDynamoDoc(hashKey.get, "lastReadTransactId", maxTransactId)
    }

  }

  def processDailyDeltaLoad(): Unit = {
    val filepath1 = s"$s3FolderPath/${dateTimeInfo.yesterdayYear}/${dateTimeInfo.yesterdayMonth}/${dateTimeInfo.yesterdayDate}/*"
    val filepath2 = s"$s3FolderPath/${dateTimeInfo.currentYear}/${dateTimeInfo.currentMonth}/${dateTimeInfo.currentDate}/*"
    val combinedDF = readCurrentAndLastFilePaths(filepath1, filepath2)
    if (combinedDF.isEmpty) {
      logger.warn(s"DataFrame is empty. Exiting the code.")
      sys.exit(1)
    }
    else {
      val (latestRecords, maxTransactId) = extractLatestRecords(combinedDF, naturalKeys,dataLakeCols, "TRANSACT_ID")
      val transformedDF = transform(latestRecords)
      reloadToJDBC(transformedDF, deltaTableName)
      processCDCRecords()
      insertOrUpdateDynamoDoc(hashKey.get, "lastReadTransactId", maxTransactId)
    }

  }

  def processMonthlyDeltaLoad(): Unit = {
    val filepath1 = s"$s3FolderPath/${dateTimeInfo.previousMonthYear}/${dateTimeInfo.previousMonth}/*/*"
    val filepath2 = s"$s3FolderPath/${dateTimeInfo.currentYear}/${dateTimeInfo.currentMonth}/*/*"
    val combinedDF = readCurrentAndLastFilePaths(filepath1, filepath2)
    if (combinedDF.isEmpty) {
      println("DataFrame is empty. Exiting the code.")
      sys.exit(1)
    }
    else {
      val (latestRecords, maxTransactId) = extractLatestRecords(combinedDF, naturalKeys,dataLakeCols, "TRANSACT_ID")
      val transformedDF = transform(latestRecords)
      reloadToJDBC(transformedDF, deltaTableName)
      processCDCRecords()
      insertOrUpdateDynamoDoc(hashKey.get, "lastReadTransactId", maxTransactId)
    }

  }

  def processYearlyDeltaLoad(): Unit = {
    val filepath1 = s"$s3FolderPath/${dateTimeInfo.previousYear}/*/*/*"
    val filepath2 = s"$s3FolderPath/${dateTimeInfo.currentYear}/*/*/*"
    val combinedDF = readCurrentAndLastFilePaths(filepath1, filepath2)
    if (combinedDF.isEmpty) {
      println("DataFrame is empty. Exiting the code.")
      sys.exit(1)
    }
    else {
      val (latestRecords, maxTransactId) = extractLatestRecords(combinedDF, naturalKeys,dataLakeCols, "TRANSACT_ID")
      val transformedDF = transform(latestRecords)
      reloadToJDBC(transformedDF, deltaTableName)
      processCDCRecords()
      insertOrUpdateDynamoDoc(hashKey.get, "lastReadTransactId", maxTransactId)
    }

  }

  def processCDCRecords(): Unit = {
    logger.info("Start processing CDC records from Delta Table")
    val columns = targetTableCols.map(column => column).mkString(", ")
    val columnListWithPrefixSRC = targetTableCols.map(column => s"SRC.$column").mkString(", ")
    val updateCondition: String = targetTableNaturalKeys.map(column => s"$targetTableName.$column = SRC.$column").mkString(" AND ")
    val updateCols: String = targetTableNonNaturalKeys.map(column => s"$column = SRC.$column").mkString(", ")
    logger.info("Start First Step.....")
    jdbcHandler.executeQuery(s"INSERT INTO $targetTableName ($columns) SELECT $columnListWithPrefixSRC FROM $deltaTableName SRC WHERE SRC.OP = 'I'  ;")
    logger.info("Start Second Step.....")
    jdbcHandler.executeQuery(s"INSERT INTO $targetTableName ($columns) SELECT $columnListWithPrefixSRC FROM $deltaTableName SRC WHERE SRC.OP = 'U'  AND NOT EXISTS (    SELECT 1   FROM $targetTableName    WHERE $updateCondition  );")
    logger.info("Start Third Step.....")
    jdbcHandler.executeQuery(s"UPDATE $targetTableName SET  $updateCols FROM $deltaTableName SRC WHERE $updateCondition  AND SRC.OP = 'U';")
    logger.info("Start Fourth Step.....")
    jdbcHandler.executeQuery(s"DELETE FROM $targetTableName USING $deltaTableName SRC WHERE $updateCondition  AND SRC.OP = 'D';")

  }

  def shouldRunInitialLoad(): Boolean = {
    dynamoDBHandler.countDynamoDocRecords(hashKey.get) == 0 || dynamoDBHandler.extractRecordForHashKey(hashKey.get, "lastReadTransactId") == "-1"
  }

  def insertOrUpdateDynamoDoc(hashKey: String, attributeName: String, attributeValue: String): Unit = {
    dynamoDBHandler.insertRecordDynamoDocument(hashKey, attributeName, attributeValue)
  }

  def extractInitialLoadParquet(columns: Seq[String]): DataFrame = {
    logger.info(s"Extracting bulk load parquet from s3")
    extractParquetFromS3(s3FilePathFullLoad).select(columns.map(col): _*)
  }

  def extractMaxTransactId(dataFrame: DataFrame): String = {
    logger.info(s"Fetching max transact id from dynamo db")
    dataFrame.agg(max("TRANSACT_ID").alias("MAX_TRANSACT_ID")).collect()(0).getAs[String]("MAX_TRANSACT_ID")
  }


  def readDeltaLoadRecords(path: String): DataFrame = {
    logger.info(s"Reading delta records  from s3")
    s3Handler.readParquetFile(path)
  }

  def extractLatestRecords(dataFrame: DataFrame, naturalKeys: Seq[String], dataLakeCols: Seq[String] ,aggregateCol: String ): (DataFrame, String) = {
    logger.info(s"Grouping latest records based on natural keys")
    val latestCustCodes = dataFrame.groupBy(naturalKeys.map(col): _*).agg(max(aggregateCol).alias("TRANSACT_ID"))
    val colsList = "Op" +: dataLakeCols
    (dataFrame.join(latestCustCodes, naturalKeys :+ "TRANSACT_ID").select(colsList.map(col): _*), extractMaxTransactId(latestCustCodes))
  }



  def readCurrentAndLastFilePaths(filepath1: String, filepath2: String):DataFrame = {
    logger.info(s"Reading from last two folders")
    val fileExistsPreviousHour = s3Handler.checkFileExists(filepath1)
    val fileExistsCurrentHour = s3Handler.checkFileExists(filepath2)
    val lasttransactIdValue: String = dynamoDBHandler.extractRecordForHashKey(hashKey.get, "lastReadTransactId")

    val combinedDF: DataFrame = if (fileExistsCurrentHour && fileExistsPreviousHour) {
      logger.info(s"Files exist in both folders")
      s3Handler.readMultipleParquetFiles(filepath1, filepath2).filter(col("TRANSACT_ID") > lasttransactIdValue)

    } else if (fileExistsCurrentHour) {
      logger.info(s"Files exist in just current folder")
      s3Handler.readParquetFile(filepath2)
        .filter(col("TRANSACT_ID") > lasttransactIdValue)
    }
    else if (fileExistsPreviousHour) {
      logger.info(s"Files exist in just previous folder")
      s3Handler.readParquetFile(filepath1)
        .filter(col("TRANSACT_ID") > lasttransactIdValue)
    }
    else {
      logger.info(s"Files doesn't exist in current or  previous folder")
      spark.createDataFrame(Seq.empty[(String, Int)])
    }

    combinedDF
  }




}
