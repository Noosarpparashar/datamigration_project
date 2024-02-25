package com.its.esales.batch.productinfo

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.Regions
import com.its.esales.batch.common.DMSHandler
import org.apache.spark.sql.DataFrame

object ProductInfo extends DMSHandler with App {
  override lazy val awsCredentials = new BasicAWSCredentials("AKIAYS2NSSMZDMCFVDV7", "AHAXqOcOOTdUQ1qrZxN6AUaYdqbwFU/purHNllih")
  override lazy val region = Some(Regions.US_EAST_1)
  override lazy val dynamoDocName = Some("audit_ecart_tables")
  override lazy val hashKeyColumn = Some("schemaname_tablename")
  override lazy val hashKey = Some("ecart_productinfo")
  override lazy val jdbcUrl = "jdbc:redshift://test-workgroup.590183830322.us-east-1.redshift-serverless.amazonaws.com:5439/its"
  override lazy val jdbcUser = "admin"
  override lazy val jdbcPassword = "Admin12345678"
  override lazy val targetTableName = "ecart_tgt.productinfo"
  override lazy val deltaTableName = "cdc.cd_productinfo"
  override lazy val s3FolderPath = "ecart/ecart/productinfo"
  override lazy val bucketName = "datalake-youtube-itstreamer"
  override lazy val dataLakeColumns = Some(Seq("PRODUCTID", "PRODUCTNAME", "PRODCAT", "STOREID", "PRODUCTPRICE"))
  override lazy val naturalKeyList = Some(Seq("PRODUCTID"))
  override lazy val targetTableColumns = Some(Seq("PRODUCTID", "PRODUCTNAME", "PRODCAT", "STOREID", "PRODUCTPRICE"))
  override lazy val targetTableNaturalKeysList = Some(Seq("PRODUCTID"))

  override def transform(df: DataFrame): DataFrame = {
    df
  }




  processLoad("Hourly")

}
//7 mins