package com.its.esales.testCodes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object MatchFiles extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]").appName("SparkByExamples3.com")
    .getOrCreate()

  val src = spark.read.option("header", "true").option("multiLine", "true").csv("C:\\Users\\paras\\customer_202401071827.csv")
  val tgt = spark.read.option("header", "true").option("multiLine", "true").csv("C:\\Users\\paras\\customer_202401071828.csv").toDF("custidtgt", "custnametgt", "custaddtgt")

  src.show()
  tgt.show()
  src.join(tgt,src("custid")===tgt("custidtgt"), "leftouter").filter(col("custidtgt").isNull).show()



}
