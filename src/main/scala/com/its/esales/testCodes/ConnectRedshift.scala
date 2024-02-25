package com.its.esales.testCodes
import java.sql.{Connection, DriverManager, Statement}
import java.util.Properties
import scala.util.{Failure, Success, Try}

object ConnectRedshift extends App{

   def connect(): Try[Connection] = {
      try {
        println(">>>> Connecting to Redshift...")

        val props = new Properties()
        props.setProperty("user", "admin")
        props.setProperty("password", "Admin12345678")

        val newConn = DriverManager.getConnection("jdbc:redshift://test-workgroup.590183830322.us-east-1.redshift-serverless.amazonaws.com:5439/dev", props)
        println(">>>> Connected.")

        Success(newConn)
      } catch {
        case ex: Exception => {
          print(ex.toString)
          Failure(ex)
        }
      }
    }

    connect()


}
