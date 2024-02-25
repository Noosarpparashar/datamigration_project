package com.its.esales.testCodes


import com.amazonaws.auth.{AWSCredentials, AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.regions.Regions
import com.amazonaws.services.secretsmanager.model.{GetSecretValueRequest, GetSecretValueResult}
import com.amazonaws.services.secretsmanager.{AWSSecretsManager, AWSSecretsManagerClient, AWSSecretsManagerClientBuilder}
import com.amazonaws.thirdparty.jackson.databind.ObjectMapper
import com.its.esales.framework.util.encryption.Decryptor
import com.its.esales.testCodes.ConnectDynamoDb.awsCredentials


object GetSecrets extends App  {
  val decryptor = new Decryptor

  val awsCredentials = new BasicAWSCredentials("AKIAYS2NSSMZDMCFVDV7", "AHAXqOcOOTdUQ1qrZxN6AUaYdqbwFU/purHNllih")
  lazy val region = Regions.US_EAST_1
  val client: AWSSecretsManager = AWSSecretsManagerClientBuilder.standard()
    .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
    .withRegion(region)
    .build()

  def getAndDecryptSecret(secretId: String, key1:String): String = {

    val getSecretValueRequest = new GetSecretValueRequest().withSecretId(secretId)
    val getSecretValueResult: GetSecretValueResult = client.getSecretValue(getSecretValueRequest)
    println ("Secret retrieved****************** ")
    val secretBinaryString = getSecretValueResult.getSecretString
    print("This is my secret string 29",secretBinaryString)

    val objectMapper = new ObjectMapper()

    val secretMap = objectMapper.readValue(secretBinaryString, classOf[java.util.HashMap[String, String]])

    val decryptedString = decryptor.decryptEncodedData(secretMap.get(key1))

    decryptedString


  }

  def retrieveFromSecrets(secretId: String, key1:String): String = {

    val getSecretValueRequest = new GetSecretValueRequest().withSecretId(secretId)
    val getSecretValueResult: GetSecretValueResult = client.getSecretValue(getSecretValueRequest)
    val secretBinaryString = getSecretValueResult.getSecretString
    val objectMapper = new ObjectMapper()
    val secretMap = objectMapper.readValue(secretBinaryString, classOf[java.util.HashMap[String, String]])
    print(secretMap.get(key1))
    secretMap.get(key1)


  }


//
//  val a = getAndDecryptSecret("prod/ecart_migration/jdbcDB", "myjdbcPassword2")
//  println(a)
//  val b = retrieveFromSecrets("prod/ecart_migration/jdbcDB","myjdbcURL")
//  println(b)

//  val decryptedString = decryptor.decryptEncodedData("AQICAHhkqy+tYX+jOLSlZ1BYFeLvejAZ/+z/S0b7ztIBkDQwewHX8YnJfjZ90nbf52mLrXanAAAAazBpBgkqhkiG9w0BBwagXDBaAgEAMFUGCSqGSIb3DQEHATAeBglghkgBZQMEAS4wEQQM/qkcQ0PPKjIyr0OVAgEQgCjI0o7vlb1LCFvQwY0Wb9mhGHkoFaCFz/r9/1sCnJvNYtexnFdTV3OH")
//
//  println(decryptedString)


}
