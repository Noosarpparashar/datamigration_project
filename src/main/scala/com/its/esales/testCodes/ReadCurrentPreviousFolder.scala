package com.its.esales.testCodes

import org.apache.spark.sql.SparkSession
import java.time.{LocalDateTime, ZoneOffset, Instant}
import java.time.format.DateTimeFormatter

// Get the current date and time in UTC



object ReadCurrentPreviousFolder extends App{

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]").appName("SparkByExamples3.com")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  spark.sparkContext
    .hadoopConfiguration.set("fs.s3a.access.key", "AKIAYS2NSSMZDMCFVDV7")
  spark.sparkContext
    .hadoopConfiguration.set("fs.s3a.secret.key", "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")
  spark.sparkContext
    .hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")




  val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy, MM, dd, HH")

  val currentDateTime: LocalDateTime = LocalDateTime.now(ZoneOffset.UTC)

  // Format the LocalDateTime
  val formattedDate: String = currentDateTime.format(formatter)

  println(formattedDate)

  val currentYear = currentDateTime.getYear
  val currentMonth = currentDateTime.format(DateTimeFormatter.ofPattern("MM"))
  val currentDate = currentDateTime.format(DateTimeFormatter.ofPattern("dd"))
  val currentHour = currentDateTime.format(DateTimeFormatter.ofPattern("HH"))

  val lastHourDateTime = currentDateTime.minusHours(1)
  val lastHourYear = lastHourDateTime.getYear
  val lastHourMonth = lastHourDateTime.format(DateTimeFormatter.ofPattern("MM"))
  val lastHourDate = lastHourDateTime.format(DateTimeFormatter.ofPattern("dd"))
  val lastHour = lastHourDateTime.format(DateTimeFormatter.ofPattern("HH"))

//  println(s"Current Year: $currentYear")
//  println(s"Current Month: $currentMonth")
//  println(s"Current Date: $currentDate")
//  println(s"Current Hour: $currentHour")
//
//  println(s"Last Hour Year: $lastHourYear")
//  println(s"Last Hour Month: $lastHourMonth")
//  println(s"Last Hour Date: $lastHourDate")
//  println(s"Last Hour: $lastHour")
//
//  println(currentYear, currentMonth, currentDate, currentHour)
//  println(lastHourYear, lastHourMonth, lastHourDate, lastHour)

  val baseDirectory = "s3a://datalake-youtube-itstreamer/ecart/ecart/customer/"

  val filePathPattern1 = s"$baseDirectory/$currentYear/$currentMonth/$currentDate/$currentHour/2*.parquet"
  val filePathPattern2 = s"$baseDirectory/$lastHourYear/$lastHourMonth/$lastHourDate/$lastHour/2*.parquet"

  //println(filePathPattern1)


  val cdcRecords = spark.read.format("parquet")
    .option("inferSchema", "true")
    .load(filePathPattern2)



  // Specify the file path pattern using the provided values


  // Read CSV files matching the pattern
  cdcRecords.show()
  println(cdcRecords.count())

}
