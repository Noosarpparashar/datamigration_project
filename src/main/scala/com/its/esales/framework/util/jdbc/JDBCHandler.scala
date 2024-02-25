package com.its.esales.framework.util.jdbc
import com.its.esales.framework.jobs.controllers.BaseController
import com.its.esales.framework.traits.Logging
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.sql.{Connection, DriverManager, PreparedStatement, Statement}

class JDBCHandler(jdbcUrl: String, user: String, password: String) extends AutoCloseable with BaseController with Logging{
  val props = new java.util.Properties()
  props.setProperty("user", user)
  props.setProperty("password", password)

  val connection: Connection = DriverManager.getConnection(jdbcUrl, props)

  def executeQuery(sql: String): Unit = {
    try {
      // Execute the provided SQL query
      val statement: Statement = connection.createStatement()
      statement.executeUpdate(sql)
      logger.info(s"Query  $sql executed successfully.")
    } catch {
      case ex: Exception =>
        logger.error(s"Error executing query: $sql. Details: ${ex.getMessage}")
        throw ex
    }
  }

  def extractFromJDBC(tableName:String):
  DataFrame = {
    spark.read.jdbc(jdbcUrl,tableName, props)

  }

  def extractQueryFromJDBC(query:String,numPartitions:Int = 10, fetchsize:Int = 10000 ):
  DataFrame = {
    spark.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("numPartitions", 10)
      .option("fetchsize", 10000)
      .option("query", query)
      .option("user", user)
      .option("password", password)
      .load()

  }

  def extractQueryOutputFromJDBC(query: String):
  DataFrame = {
    spark.read.jdbc(jdbcUrl, "ecart_tgt.customer", props)

  }


  def executePreparedStatement(sql: String, parameters: Seq[Any]): Unit = {
    try {
      // Execute the provided SQL query with prepared statement
      val preparedStatement: PreparedStatement = connection.prepareStatement(sql)

      // Set parameters for the prepared statement
      for ((param, index) <- parameters.zipWithIndex) {
        preparedStatement.setObject(index + 1, param)
      }

      preparedStatement.executeUpdate()
      logger.info("PreparedStatement executed successfully.")
    } catch {
      case ex: Exception =>
        logger.error(s"Error executing PreparedStatement: ${ex.getMessage}")
        throw ex
    }
  }

  def appendDataFrameToJDBC(df: DataFrame, targetTableName: String): Unit = {
    try {
      df.write
        .mode(SaveMode.Append)
        .jdbc(jdbcUrl, targetTableName, props)
    }
    catch {
      case ex: Exception =>
        logger.error(s"Error executing PreparedStatement: ${ex.getMessage}")
        throw ex
    }

  }

  def overwriteDataFrameToJDBC(df: DataFrame, tableName: String): Unit = {
    try {
      val truncateDeltaTableQuery = s"TRUNCATE TABLE $tableName"
      val statement: Statement = connection.createStatement()
      statement.executeUpdate(truncateDeltaTableQuery)
      logger.info(s"Table $tableName truncated successfully.")
      logger.info(s"Inserting dataframe to $tableName")
      df.write
        .mode(SaveMode.Append)
        .jdbc(jdbcUrl, tableName, props)
    }
    catch {
      case ex: Exception =>
        logger.error(s"Error executing PreparedStatement: ${ex.getMessage}")
        throw ex
    }

  }


  override def close(): Unit = {
    if (connection != null && !connection.isClosed) {
      connection.close()
    }
  }
}