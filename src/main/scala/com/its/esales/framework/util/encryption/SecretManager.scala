package com.its.esales.framework.util.encryption

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.regions.Regions
import com.amazonaws.services.secretsmanager.model.{GetSecretValueRequest, GetSecretValueResult}
import com.amazonaws.services.secretsmanager.{AWSSecretsManager, AWSSecretsManagerClient, AWSSecretsManagerClientBuilder}
import com.amazonaws.thirdparty.jackson.databind.ObjectMapper


class SecretManager extends Decryptor {

  val awsCredentials = new BasicAWSCredentials("AKIAYS2NSSMZDMCFVDV7", "AHAXqOcOOTdUQ1qrZxN6AUaYdqbwFU/purHNllih")

  lazy val region = Regions.US_EAST_1
  val client: AWSSecretsManager = AWSSecretsManagerClientBuilder.standard()
    .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
    .withRegion(region)
    .build()

  def getAndDecryptSecret(secretId: String, key:String): String = {

    val getSecretValueRequest = new GetSecretValueRequest().withSecretId(secretId)
    val getSecretValueResult: GetSecretValueResult = client.getSecretValue(getSecretValueRequest)
    val secretBinaryString = getSecretValueResult.getSecretString
    print(secretBinaryString)

    val objectMapper = new ObjectMapper()

    val secretMap = objectMapper.readValue(secretBinaryString, classOf[java.util.HashMap[String, String]])

    val decryptedString = decryptEncodedData(secretMap.get(key))

    decryptedString


  }

  def retrieveFromSecrets(secretId: String, key: String): String = {

    val getSecretValueRequest = new GetSecretValueRequest().withSecretId(secretId)
    val getSecretValueResult: GetSecretValueResult = client.getSecretValue(getSecretValueRequest)
    val secretBinaryString = getSecretValueResult.getSecretString
    val objectMapper = new ObjectMapper()
    val secretMap = objectMapper.readValue(secretBinaryString, classOf[java.util.HashMap[String, String]])
    print(secretMap.get(key))
    secretMap.get(key)


  }



}
