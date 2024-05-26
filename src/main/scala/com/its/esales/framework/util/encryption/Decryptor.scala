package com.its.esales.framework.util.encryption

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.regions.Regions
import com.amazonaws.services.kms.AWSKMSClientBuilder
import org.apache.spark.sql.SparkSession

import java.util.Base64
import java.nio.ByteBuffer


class Decryptor {
  val accessKey = sys.env.getOrElse("AWS_ACCESS_KEY", "")
  val secretKey = sys.env.getOrElse("AWS_SECRET_ACCESS_KEY", "")

  val awsCredentials1 = new BasicAWSCredentials(accessKey, secretKey)


  val kmsClient = AWSKMSClientBuilder.standard()
    .withCredentials(new AWSStaticCredentialsProvider(awsCredentials1))
    .withRegion(Regions.US_EAST_2)
    .build()

  def decodeData(encodedData: String) = {
    Base64.getDecoder.decode(encodedData)
  }

  def decryptEncodedData (encodedData: String): String = {
    val decryptRequest = new com.amazonaws.services.kms.model.DecryptRequest()
      .withKeyId(sys.env("CUSTOMER_MASTER_KEY"))
      .withCiphertextBlob(ByteBuffer.wrap(decodeData(encodedData)))

    val decryptedString = new String(kmsClient.decrypt(decryptRequest).getPlaintext.array())

    decryptedString

  }


}
