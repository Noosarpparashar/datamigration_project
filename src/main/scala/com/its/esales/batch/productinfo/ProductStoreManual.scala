package com.its.esales.batch.productinfo

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.Regions
import com.its.esales.batch.common.BatchETL
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, col, concat, concat_ws, expr, lit, udf}

import java.util.UUID

class ProductStoreManual extends BatchETL  {
  override lazy val awsCredentials = new BasicAWSCredentials(sys.env.getOrElse("AWS_ACCESS_KEY", ""), sys.env.getOrElse("AWS_SECRET_KEY", ""))
  override lazy val region = Some(Regions.US_EAST_1)
  override lazy val bucketName = "datalake-youtube-itstreamer"
  override lazy val jdbcUrl = getConfig("myjdbcURL")
  override lazy val jdbcUser = "admin"
  override lazy val jdbcPassword = getPassword("myjdbcPassword2")
   override def run(): Unit = {




    def smallHash(inputString: String): String = {

      val digest = java.security.MessageDigest.getInstance("SHA-256")
      val hashBytes = digest.digest(inputString.getBytes("UTF-8"))
      val hexString = hashBytes.map("%02x".format(_)).mkString
      hexString.substring(0, 10)
    }

    val manualstoreproductdf = extractCSVFromS3("csv-manual")
    manualstoreproductdf.show()
    val smallHashUDF = udf(smallHash _)
    val storeproduct = manualstoreproductdf
      .withColumn("storeid", smallHashUDF(concat(col("storename"), col("storeaddress"))))

    storeproduct.show()

    val storesdf = storeproduct.select("storeid", "storename", "storeaddress")

      .toDF("storeid", "storename", "storeadd")
    loadToJDBC(storesdf, "CDC.MN_STOREINFO")
    jdbcHandler.executeQuery("INSERT INTO ecart_tgt.STOREINFO (STOREID, STORENAME,STOREADD)   \nSELECT STOREID, STORENAME,STOREADD \nFROM CDC.MN_STOREINFO \nWHERE NOT EXISTS (\n    SELECT 1\n    FROM ecart_tgt.STOREINFO\n    WHERE ecart_tgt.STOREINFO.storeid = CDC.MN_STOREINFO.storeid\n)")
    jdbcHandler.executeQuery("TRUNCATE TABLE CDC.MN_STOREINFO")

    val productDF = storeproduct.select("productname", "productCategory", "productprice", "storeid")
      .withColumn("productid", smallHashUDF(concat(coalesce(col("productname"), lit("null")), coalesce(col("productCategory"), lit("null")), coalesce(col("storeid"), lit("null")))))
      .toDF("productname", "prodcat", "productprice", "storeid", "productid")

    productDF.show(false)
    println(productDF.count())
    loadToJDBC(productDF, "CDC.MN_PRODUCTINFO")
    jdbcHandler.executeQuery("INSERT INTO ecart_tgt.productinfo (productid, productname,prodcat,storeid,productprice)   \nSELECT productid, productname,prodcat,storeid,productprice \nFROM CDC.MN_PRODUCTINFO\nWHERE NOT EXISTS (\n    SELECT 1\n    FROM ecart_tgt.productinfo\n    WHERE ecart_tgt.productinfo.productid = CDC.MN_PRODUCTINFO.productid\n)")
    jdbcHandler.executeQuery("TRUNCATE TABLE CDC.MN_PRODUCTINFO")
  }
}
