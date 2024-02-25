package com.its.esales.testCodes

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.document.{DynamoDB, Item, Table}
import com.amazonaws.services.dynamodbv2.model.{AttributeValue, GetItemRequest, PutItemRequest}
import com.amazonaws.services.dynamodbv2.model.{DescribeTableRequest, DescribeTableResult}
import com.amazonaws.services.dynamodbv2.document.spec.{PutItemSpec, QuerySpec}
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap
import com.amazonaws.services.dynamodbv2.model.{AttributeValue, QueryRequest}
import com.amazonaws.services.dynamodbv2.model.AttributeValue


import scala.collection.JavaConverters._


object ConnectDynamoDb extends App{




        // Specify your AWS credentials and region
        val awsCredentials = new BasicAWSCredentials("AKIAYS2NSSMZDMCFVDV7", "AHAXqOcOOTdUQ1qrZxN6AUaYdqbwFU/purHNllih")
        val region = Regions.US_EAST_1 // Replace with your desired AWS region

        // Create a DynamoDB client

  val dynamoDBClient = AmazonDynamoDBClientBuilder.standard()
    .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
    .withRegion(region)
    .build()
    val tableName = "audit_ecart_tables"
    val partitionKeyValue = "ecart_customer"
    val dynamoDB = new DynamoDB(dynamoDBClient)

    // Perform a query to check if the partition key value exists
    val queryRequest = new QueryRequest()
      .withTableName(tableName)
      .withKeyConditionExpression("schemaname_tablename = :v_partition_key")
      .withExpressionAttributeValues(Map(":v_partition_key" -> new AttributeValue(partitionKeyValue)).asJava)

    val queryResult = dynamoDBClient.query(queryRequest)

    // Check if the item exists
    if (queryResult.getCount == 0) {
      // Item doesn't exist, create it
      println(s"Item with partition key $partitionKeyValue not found. Creating the item.")

      val table: Table = dynamoDB.getTable(tableName)

      val putItemSpec = new PutItemSpec()
        .withItem(new Item()
          .withPrimaryKey("schemaname_tablename", partitionKeyValue)
          // Add other attributes as needed
          .withString("lastReadTransactId", "20240107")

        )

      table.putItem(putItemSpec)
      println(s"Item with partition key $partitionKeyValue created.")
    } else {
      // Item exists, print the results
      println(s"Item with partition key $partitionKeyValue already exists.")
      val lastReadTransactId: String = queryResult.getItems.asScala.headOption
        .flatMap(item => Option(item.get("lastReadTransactId")).map(_.getS))
        .getOrElse {
          // Default value or error handling if needed
          throw new NoSuchElementException("lastReadTransactId not found in DynamoDB query result")
        }

      println(s"lastReadTransactId: $lastReadTransactId")

    }

    // Close the DynamoDB client
    dynamoDBClient.shutdown()


}
