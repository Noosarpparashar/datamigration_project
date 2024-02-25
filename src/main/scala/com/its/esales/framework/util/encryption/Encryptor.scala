package com.its.esales.framework.util.encryption

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.regions.Regions
import com.amazonaws.services.kms.AWSKMSClientBuilder
import org.apache.spark.sql.SparkSession

import java.nio.ByteBuffer

object Encryptor extends App {
  // Initialize SparkSession (eagerly)
  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("EcartSalesEncryptor" +

      "")
    .getOrCreate()
  lazy val awsCredentials = new BasicAWSCredentials("AKIAYS2NSSMZDMCFVDV7", "AHAXqOcOOTdUQ1qrZxN6AUaYdqbwFU/purHNllih")


  val kmsClient = AWSKMSClientBuilder.standard()
    .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
    .withRegion(Regions.US_EAST_1)
    .build()

  // Define plaintext data (eagerly)
  val plaintextData = "Admin12345678"
  val plaintextByteBuffer = ByteBuffer.wrap(plaintextData.getBytes())

  // Define a lazy val for customer master key
  lazy val customerMasterKey: String = sys.env.getOrElse("CUSTOMER_MASTER_KEY", "")

  // Encrypt using AWS KMS
  val encryptRequest = new com.amazonaws.services.kms.model.EncryptRequest()
    .withKeyId(customerMasterKey)
    .withPlaintext(plaintextByteBuffer)
println(encryptRequest)
  val encryptResponse = kmsClient.encrypt(encryptRequest)
  val encryptedData = encryptResponse.getCiphertextBlob.array()

  // Convert encrypted data to Base64-encoded string (eagerly)
  val base64EncryptedData: String = java.util.Base64.getEncoder.encodeToString(encryptedData)

  println("*******************************************",encryptedData)
  println(s"Encrypted Data (Base64): $base64EncryptedData")

  spark.stop()


}
