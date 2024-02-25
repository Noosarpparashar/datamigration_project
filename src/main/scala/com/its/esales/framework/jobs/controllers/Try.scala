package com.its.esales.framework.jobs.controllers

import org.apache.spark.sql.SparkSession

object Try extends App {
  val spark: SparkSession = SparkSession.builder()
    .master("local[1]").appName("SparkByExamples.com")
    .getOrCreate()
  spark.sparkContext.setLogLevel("INFO")

  import spark.implicits._

  val columns = Seq("language", "users_count")
  val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))

  val rdd = spark.sparkContext.parallelize(data)

  val dfFromRDD1 = rdd.toDF()
  dfFromRDD1.show()

}
