package com.its.esales.framework.util.dynamodb

import com.amazonaws.auth.{AWSCredentials, AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.document.{DynamoDB, Item, Table}
import com.amazonaws.services.dynamodbv2.model.{AttributeValue, DescribeTableRequest, DescribeTableResult, GetItemRequest, PutItemRequest, QueryRequest, QueryResult}
import com.amazonaws.services.dynamodbv2.document.spec.{PutItemSpec, QuerySpec}
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap

import scala.collection.JavaConverters._

class DynamoDBHandler (dynamoDocName: String, hashKeyColumnName:String, awsCredentials: BasicAWSCredentials, awsregion: Regions = Regions.US_EAST_1 ) extends AutoCloseable{


  val dynamoDBClient = AmazonDynamoDBClientBuilder.standard()
    .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
    .withRegion(awsregion)
    .build()

  val dynamoDB: DynamoDB = new DynamoDB(dynamoDBClient)

  def insertRecordDynamoDocument(hashKeyValue:String, attributeName: String, attributeValue: String): Unit = {

    val table: Table = dynamoDB.getTable(dynamoDocName)
    val putItemSpec = new PutItemSpec()
      .withItem(new Item()
        .withPrimaryKey(hashKeyColumnName, hashKeyValue)
        .withString(attributeName, attributeValue))

    table.putItem(putItemSpec)
    println(s"Item with  key $hashKeyValue inserted in  $hashKeyColumnName in $dynamoDocName created with value $attributeValue")
  }

  def performQueryToCheckPartitionKey(hashKey: String): QueryResult = {

    val queryRequest = new QueryRequest()
      .withTableName(dynamoDocName)
      .withKeyConditionExpression(s"$hashKeyColumnName = :v_partition_key")
      .withExpressionAttributeValues(Map(":v_partition_key" -> new AttributeValue(hashKey)).asJava)

    val queryResult: QueryResult = dynamoDBClient.query(queryRequest)
    queryResult
  }

  def countDynamoDocRecords(hashKey: String): Int = {

    val queryRequest = new QueryRequest()
      .withTableName(dynamoDocName)
      .withKeyConditionExpression(s"$hashKeyColumnName = :v_partition_key")
      .withExpressionAttributeValues(Map(":v_partition_key" -> new AttributeValue(hashKey)).asJava)

    val queryResult: QueryResult = dynamoDBClient.query(queryRequest)
    queryResult.getCount
  }

  def extractRecordForHashKey(hashKey: String, attributeName: String): String = {
    val queryResult = performQueryToCheckPartitionKey(hashKey )
    val value: String = queryResult.getItems.asScala.headOption
      .flatMap(item => Option(item.get(attributeName: String)).map(_.getS))
      .getOrElse {
        // Default value or error handling if needed
        throw new NoSuchElementException(s"$hashKeyColumnName not found in DynamoDB query result")
      }

    value
  }

   override def close(): Unit = {
    dynamoDBClient.shutdown()
  }



}
