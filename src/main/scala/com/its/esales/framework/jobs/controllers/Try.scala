package com.its.esales.framework.jobs.controllers

import org.apache.spark.sql.SparkSession

object Try extends App {
  val spark: SparkSession = SparkSession.builder()
    //.master("local[1]")
    .appName("SparkByExamples4.com")
    .getOrCreate()
 // spark.sparkContext.setLogLevel("INFO")

  import spark.implicits._
  println("***********************2222222222**************")

  val columns = Seq("language", "users_count")
  val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))

  val rdd = spark.sparkContext.parallelize(data)

  val dfFromRDD1 = rdd.toDF()
  dfFromRDD1.show()
//  val runtime = Runtime.getRuntime
//
//  // Get the maximum amount of memory available to the JVM in bytes
//  val maxMemory = runtime.maxMemory()
//
//  // Convert the maximum memory from bytes to megabytes for easier understanding
//  val maxMemoryMB = maxMemory / 1024 / 1024
//
//  // Print the maximum memory usage
//  println(s"Maximum memory available to the JVM: $maxMemoryMB MB")

}
