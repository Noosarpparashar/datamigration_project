package com.its.esales.batch.customer

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.Regions
import com.its.esales.batch.common.BatchETL

object CustomerManual extends BatchETL with App {
  override lazy val awsCredentials = new BasicAWSCredentials("AKIAYS2NSSMZDMCFVDV7", "AHAXqOcOOTdUQ1qrZxN6AUaYdqbwFU/purHNllih")
  override lazy val region = Some(Regions.US_EAST_1)
  override lazy val bucketName = "datalake-youtube-itstreamer"
  override lazy val jdbcUrl = "jdbc:redshift://test-workgroup.590183830322.us-east-1.redshift-serverless.amazonaws.com:5439/its"
  override lazy val jdbcUser = "admin"
  override lazy val jdbcPassword = "Admin12345678"

  val customerdf  = extractParquetFromS3("customerinfo/2*/*.parquet").toDF("custid", "custname", "custadd")
  println(customerdf.count())
  customerdf.show()

  loadToJDBC(customerdf, "ecart_tgt.customer" )



}
