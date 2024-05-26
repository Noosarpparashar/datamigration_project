package com.its.esales.framework.jobs.controllers

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.regions.Regions
import com.amazonaws.services.secretsmanager.model.{GetSecretValueRequest, GetSecretValueResult}
import com.amazonaws.services.secretsmanager.{AWSSecretsManager, AWSSecretsManagerClientBuilder}
import com.amazonaws.thirdparty.jackson.databind.ObjectMapper
import com.its.esales.framework.jobs.controllers.BaseController
import com.its.esales.framework.util.Redshift.RedshiftHandler
import com.its.esales.framework.util.calendar.DateTimeInfo
import com.its.esales.framework.util.dynamodb.DynamoDBHandler
import com.its.esales.framework.util.encryption.Decryptor
import com.its.esales.framework.util.jdbc.JDBCHandler
import com.its.esales.framework.util.s3.s3Handler
import org.apache.spark
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import java.time.{LocalDateTime, ZoneOffset}
import java.util.Properties

object IMDBNames extends App {
  val spark: SparkSession = SparkSession.builder()
    //.master("local[*]")
    .appName("SparkByExamples4.com")
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate()
  val accessKey = sys.env.getOrElse("AWS_ACCESS_KEY", "")
  val secretKey = sys.env.getOrElse("AWS_SECRET_ACCESS_KEY", "")
  println("******************************",accessKey)
  println("******************************",secretKey)

  spark.sparkContext
    .hadoopConfiguration.set("fs.s3a.access.key", accessKey)
  spark.sparkContext
    .hadoopConfiguration.set("fs.s3a.secret.key", secretKey)
  spark.sparkContext
    .hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")

  val decryptor = new Decryptor

  val awsCredentials = new BasicAWSCredentials(accessKey, secretKey)
  lazy val region = Regions.US_EAST_2
  val client: AWSSecretsManager = AWSSecretsManagerClientBuilder.standard()
    .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
    .withRegion(region)
    .build()

  def getAndDecryptSecret(secretId: String, key1: String): String = {

    val getSecretValueRequest = new GetSecretValueRequest().withSecretId(secretId)
    val getSecretValueResult: GetSecretValueResult = client.getSecretValue(getSecretValueRequest)
    println("Secret retrieved****************** ")
    val secretBinaryString = getSecretValueResult.getSecretString
    //print("This is my secret string 29", secretBinaryString)

    val objectMapper = new ObjectMapper()

    val secretMap = objectMapper.readValue(secretBinaryString, classOf[java.util.HashMap[String, String]])

    val decryptedString = decryptor.decryptEncodedData(secretMap.get(key1))

    decryptedString


  }

  def retrieveFromSecrets(secretId: String, key1: String): String = {

    val getSecretValueRequest = new GetSecretValueRequest().withSecretId(secretId)
    val getSecretValueResult: GetSecretValueResult = client.getSecretValue(getSecretValueRequest)
    val secretBinaryString = getSecretValueResult.getSecretString
    val objectMapper = new ObjectMapper()
    val secretMap = objectMapper.readValue(secretBinaryString, classOf[java.util.HashMap[String, String]])
    print(secretMap.get(key1))
    secretMap.get(key1)


  }


  //
    val jdbcpasswd = getAndDecryptSecret("demo/prod/creds", "demo_db_passwd")
    //println("my db password",jdbcpasswd)
    val jdbcurl = retrieveFromSecrets("demo/prod/creds","jdbc_url_demo_db")
    println("my jdbc url",jdbcurl)
  val jdbcusername= retrieveFromSecrets("demo/prod/creds", "demo_db_user")
  println("my jdbc url", jdbcusername)


  val imdbdata = spark.read.format("parquet")
    //.option("inferSchema", "true")
    //.option("multiline", "true")
    .load("s3a://myyoutube-spark-on-kubernetes/imdb_dataset_staging/*.parquet")
  imdbdata.show()
  println("This is the count", imdbdata.count())





  val props = new Properties()
  props.setProperty("user", jdbcusername)
  props.setProperty("password", jdbcpasswd)
  props.setProperty("driver", "org.postgresql.Driver")
  props.setProperty("batchsize", "500000")

  imdbdata
    .repartition(200)
    .select("nconst", "primaryName", "birthYear", "deathYear", "primaryProfession", "knownForTitles")
    .write
    .mode(SaveMode.Append)
    .jdbc(jdbcurl, "imdb.persons", props)


// spark-submit --deploy-mode client --master spark://master:7077 --class com.its.esales.framework.jobs.controllers.IMDBNames  --total-executor-cores 4 --executor-cores 2 --executor-memory 2G  ecart-migration-assembly-0.1.0-SNAPSHOT.jar
}

