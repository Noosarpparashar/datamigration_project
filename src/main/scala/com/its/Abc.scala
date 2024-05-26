package com.its

import com.its.esales.batch.productinfo.ProductStoreManual
import org.apache.spark.sql.SparkSession

object Abc {
  def main(args: Array[String]): Unit = {
    // Create SparkSession
    val spark = SparkSession.builder()
      .appName("ProductStoreETL")
      .master("local[*]") // You can adjust the master URL as needed
      .getOrCreate()
    println("Helloooooooo1111")

    try {
    } finally {
      // Stop SparkSession
      spark.stop()
    }
  }
}
