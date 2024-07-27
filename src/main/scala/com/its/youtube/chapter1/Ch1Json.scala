package com.its.youtube.chapter1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Ch1Json extends App{



  val spark = SparkSession.builder()
    .appName("E-commerce Transaction Processing")
    .master("local[*]")
    .getOrCreate()



    val rawData = spark.read    .option("multiline", "true")
      .json("src/main/scala/com/its/youtube/chapter1/datasets/transactions_*.json")

    //rawData.show(false)


  val deliveryAddress = rawData
    .withColumn("delivery_info_json", to_json(col("delivery_info")))
    .select("transaction_id", "customer_id", "delivery_info_json")
    .select(
            col("*"),
      json_tuple(col("delivery_info_json"), "status", "delivery_time", "address").as(Seq("delivery_status", "delivery_time", "address_json"))
    )
    .select("transaction_id", "customer_id", "address_json")
    .select(
            col("*"),
      json_tuple(col("address_json"), "city", "state", "street", "zip").as(Seq("city", "state", "street", "zip"))
    )
    .select("transaction_id", "customer_id", "city", "state", "street", "zip")

  //deliveryAddress.show(false)



  deliveryAddress.groupBy("customer_id").agg(count("customer_id").alias("custcnt"))
    .filter(col("custcnt")>1)
    .show()

   val completeAddress = deliveryAddress.withColumn("address",concat_ws(", ", col("street"), col("city"),col("state"),
     col("zip")))
     .select( "customer_id", "address")

  completeAddress.filter(col("customer_id")==="C006").show()

   val custAddress = completeAddress.groupBy("customer_id").agg(collect_list(col("address")))
     .filter(col("customer_id")==="C006")

  custAddress.filter(col("customer_id")==="C006").show(false)

   val itemdf = rawData.select("transaction_id", "items")
    itemdf.show(false)

   val itemdesc = itemdf.withColumn("items_description", explode(col("items"))).select("transaction_id", "items_description")
   itemdesc.show(false)


   itemdesc
     .withColumn("items_to_json", to_json(col("items_description")))
     .select(
             col("*"),
       json_tuple(col("items_to_json"), "item_id", "price", "product_name", "quantity").as(Seq("item_id", "price", "product_name", "quantity"))
     )
     .select("transaction_id", "item_id", "price", "product_name", "quantity")
     .show()











}
