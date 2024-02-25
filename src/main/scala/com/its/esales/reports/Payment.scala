package com.its.esales.reports

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.Regions
import org.apache.spark.sql.functions.{col, lit, rand, round}
import com.its.esales.batch.common.BatchETL

object Payment extends BatchETL with App{
  override lazy val awsCredentials = new BasicAWSCredentials("AKIAYS2NSSMZDMCFVDV7", "AHAXqOcOOTdUQ1qrZxN6AUaYdqbwFU/purHNllih")
  override lazy val region = Some(Regions.US_EAST_1)
  override lazy val jdbcUrl = "jdbc:redshift://test-workgroup.590183830322.us-east-1.redshift-serverless.amazonaws.com:5439/its"
  override lazy val jdbcUser = "admin"
  override lazy val jdbcPassword = "Admin12345678"

  val selectProductInfoQuery = "select PRODUCTID, PRODUCTPRICE FROM ECART_TGT.PRODUCTINFO"
  val selectOrderInfoQuery = "select ORDERID, PRODUCTID FROM ECART_TGT.ORDER"

  val productInfo =extractQueryFromJDBC(selectProductInfoQuery)
  val orderInfo =extractQueryFromJDBC(selectOrderInfoQuery)

  val paymentsDF = orderInfo
    .join(productInfo, Seq("PRODUCTID"),  "left" )
    .withColumn("DISCOUNT", round(rand() * 30, 2))
    .withColumn("PRODUCTPRICE", round(col("PRODUCTPRICE"), 2))
    .withColumn("SELLPRICE",round(col("PRODUCTPRICE")*col("DISCOUNT")/100, 2 ))
    .select("ORDERID", "PRODUCTID", "PRODUCTPRICE", "DISCOUNT", "SELLPRICE")

  reloadToJDBC(paymentsDF, "ecart_tgt.PAYMENT")



}
