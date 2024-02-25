package com.its.esales.framework.jobs

import com.typesafe.config.ConfigFactory
import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession

trait Job {
  def run(): Unit = ???


}

