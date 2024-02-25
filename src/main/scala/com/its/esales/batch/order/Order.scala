package com.its.esales.batch.order

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.Regions
import com.its.esales.batch.common.DMSHandler
import org.apache.spark.sql.DataFrame

object Order extends DMSHandler with App {
  override lazy val awsCredentials = new BasicAWSCredentials("AKIAYS2NSSMZDMCFVDV7", "AHAXqOcOOTdUQ1qrZxN6AUaYdqbwFU/purHNllih")
  override lazy val region = Some(Regions.US_EAST_1)
  override lazy val dynamoDocName = Some("audit_ecart_tables")
  override lazy val hashKeyColumn = Some("schemaname_tablename")
  override lazy val hashKey = Some("ecart_order")
  override lazy val jdbcUrl = "jdbc:redshift://test-workgroup.590183830322.us-east-1.redshift-serverless.amazonaws.com:5439/its"
  override lazy val jdbcUser = "admin"
  override lazy val jdbcPassword = "Admin12345678"
  override lazy val targetTableName = "ecart_tgt.order"
  override lazy val deltaTableName = "cdc.cd_order"
  override lazy val s3FolderPath = "ecart/ecart/fact_order"
  override lazy val bucketName = "datalake-youtube-itstreamer"
  override lazy val dataLakeColumns = Some(Seq("ORDERID", "CUSTID", "PRODUCTID", "PURCHASETIMESTAMP"))
  override lazy val naturalKeyList = Some(Seq("ORDERID"))
  override lazy val targetTableColumns = Some(Seq("ORDERID", "CUSTID", "PRODUCTID", "PURCHASETIMESTAMP"))
  override lazy val targetTableNaturalKeysList = Some(Seq("ORDERID"))

  override def transform(df: DataFrame): DataFrame = {
    df.show()
    df
  }




  processLoad("Hourly")
  //11 mins

}