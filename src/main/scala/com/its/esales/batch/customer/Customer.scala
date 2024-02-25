package com.its.esales.batch.customer
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.Regions
import com.its.esales.batch.common.DMSHandler
import org.apache.spark.sql.{DataFrame, Row}
object  Customer extends DMSHandler with App {
  override lazy val awsCredentials = new BasicAWSCredentials("AKIAYS2NSSMZDMCFVDV7", "AHAXqOcOOTdUQ1qrZxN6AUaYdqbwFU/purHNllih")
  override lazy val region = Some(Regions.US_EAST_1)
  override lazy val dynamoDocName = Some("audit_ecart_tables")
  override lazy val hashKeyColumn = Some("schemaname_tablename")
  override lazy val hashKey = Some("ecart_customer")
  override lazy val jdbcUrl = "jdbc:redshift://test-workgroup.590183830322.us-east-1.redshift-serverless.amazonaws.com:5439/its"
  override lazy val jdbcUser = "admin"
  override lazy val jdbcPassword = "Admin12345678"
  override lazy val targetTableName = "ecart_tgt.customer"
  override lazy val deltaTableName = "cdc.cd_customer"
  override lazy val s3FolderPath = "ecart/ecart/customer"
  override lazy val bucketName = "datalake-youtube-itstreamer"
  override lazy val dataLakeColumns = Some(Seq("custid", "custname", "custadd"))
  override lazy val naturalKeyList = Some(Seq("custid"))
  override lazy val targetTableColumns = Some(Seq("CUSTID", "CUSTNAME", "CUSTADD"))
  override lazy val targetTableNaturalKeysList = Some(Seq("CUSTID"))

  override def transform(df: DataFrame): DataFrame = {
  df.show()
    df
  }




  processLoad("Hourly")

}
