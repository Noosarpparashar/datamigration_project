package com.its.esales.framework.jobs.controllers

import com.its.esales.framework.jobs.{Job, JobEnum, JobFactory}
import org.apache.spark.sql.SparkSession

object ProductStoreManual extends BaseController with Job {

  def main(args: Array[String]): Unit = {
    val jobType = JobEnum.ProductStoreManual
    val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
      .appName("SparkByExamples3.com")
      .getOrCreate()



    JobFactory.createJob(spark, jobType).run()
    println("Hello from controller1")
    //spark.stop()

  }

}

