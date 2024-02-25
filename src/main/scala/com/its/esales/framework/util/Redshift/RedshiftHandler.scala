package com.its.esales.framework.util.Redshift

import com.its.esales.framework.util.jdbc.JDBCHandler

import java.sql.{ResultSet, Statement}

class RedshiftHandler(jdbcUrl: String, user: String, password: String, schema: String) extends JDBCHandler(jdbcUrl, user, password) {

  def performRedshiftSpecificOperation(customQuery: String): Unit = {
    // Example: Perform Redshift-specific operation
    val redshiftSpecificQuery = if (customQuery.nonEmpty) customQuery else s"SELECT 1 FROM $schema.someTable"
    val statement: Statement = connection.createStatement()
    val resultSet = statement.executeUpdate(redshiftSpecificQuery)

  }

  def fetchRecordsFromRedshift(customQuery: String): ResultSet = {
    // Example: Perform Redshift-specific operation
    val redshiftSpecificQuery = if (customQuery.nonEmpty) customQuery else s"SELECT 1 FROM $schema.someTable"
    val statement: Statement = connection.createStatement()
    val resultSet = statement.executeQuery(redshiftSpecificQuery)
    return resultSet

  }

  override def close(): Unit = {
    // Perform any Redshift-specific cleanup if needed

    // Close the JDBC connection
    super.close()
  }
}