package com.its.esales.framework.jobs.controllers

import org.apache.spark.sql.SparkSession

trait BaseController {

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]").appName("SparkByExamples3.com")
    .getOrCreate()

}
