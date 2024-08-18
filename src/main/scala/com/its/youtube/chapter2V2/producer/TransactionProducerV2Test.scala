import scala.io.Source
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, Callback, RecordMetadata}
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.spark.sql.SparkSession
import org.json4s._
import org.json4s.jackson.JsonMethods._

object TransactionProducerV2Test {
  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    val spark = SparkSession.builder
      .appName("KafkaProducerTest")
      .master("local[*]") // Adjust as necessary
      .getOrCreate()

    // Path to the Avro schema file
    val schemaFile = "src/main/scala/com/its/youtube/chapter2/producerschema.avsc" // Update with the actual path

    // Load Avro schema from file
    val schemaString = Source.fromFile(schemaFile).getLines().mkString
    val schema = new Schema.Parser().parse(schemaString)

    // Define Kafka properties
    val kafkaProperties = new Properties()
    kafkaProperties.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094")
    kafkaProperties.put("key.serializer", classOf[KafkaAvroSerializer].getName)
    kafkaProperties.put("value.serializer", classOf[KafkaAvroSerializer].getName)
    kafkaProperties.put("schema.registry.url", "http://localhost:8081") // Replace with your schema registry URL

    // Broadcast the schema and Kafka properties
    val broadcastSchema = spark.sparkContext.broadcast(schema)
    val broadcastKafkaProperties = spark.sparkContext.broadcast(kafkaProperties)

    // Create a single JSON record with additional data
    val jsonRecord = """{
      "accountNumber": "737265056",
      "customerId": "737265056",
      "creditLimit": 5000.0,
      "availableMoney": 5000.0,
      "transactionDateTime": "2016-08-13T14:27:32",
      "transactionAmount": 98.55,
      "merchantName": "Uber",
      "acqCountry": "US",
      "merchantCountryCode": "US",
      "posEntryMode": "02",
      "posConditionCode": "01",
      "merchantCategoryCode": "rideshare",
      "currentExpDate": "06/2023",
      "accountOpenDate": "2015-03-14",
      "dateOfLastAddressChange": "2015-03-14",
      "cardCVV": "414",
      "enteredCVV": "414",
      "cardLast4Digits": "1803",
      "transactionType": "PURCHASE",
      "echoBuffer": "",
      "currentBalance": 0.0,
      "merchantCity": "",
      "merchantState": "",
      "merchantZip": "",
      "cardPresent": false,
      "posOnPremises": "",
      "recurringAuthInd": "",
      "expirationDateKeyInMatch": false,
      "isFraud": false,
      "somenewRecord": "somethingnew"
    }"""

    // Parse JSON record
    implicit val formats: Formats = DefaultFormats
    val json = parse(jsonRecord)

    // Convert JSON to Avro record
    val record = new GenericData.Record(broadcastSchema.value)
    record.put("accountNumber", (json \ "accountNumber").extract[String])
    record.put("customerId", (json \ "customerId").extract[String])
    record.put("creditLimit", (json \ "creditLimit").extract[Float])
    record.put("availableMoney", (json \ "availableMoney").extract[Float])
    record.put("transactionDateTime", (json \ "transactionDateTime").extract[String])
    record.put("transactionAmount", (json \ "transactionAmount").extract[Float])
    record.put("merchantName", (json \ "merchantName").extract[String])
    record.put("acqCountry", (json \ "acqCountry").extract[String])
    record.put("merchantCountryCode", (json \ "merchantCountryCode").extract[String])
    record.put("posEntryMode", (json \ "posEntryMode").extract[String])
    record.put("posConditionCode", (json \ "posConditionCode").extract[String])
    record.put("merchantCategoryCode", (json \ "merchantCategoryCode").extract[String])
    record.put("currentExpDate", (json \ "currentExpDate").extract[String])
    record.put("accountOpenDate", (json \ "accountOpenDate").extract[String])
    record.put("dateOfLastAddressChange", (json \ "dateOfLastAddressChange").extract[String])
    record.put("cardCVV", (json \ "cardCVV").extract[String])
    record.put("enteredCVV", (json \ "enteredCVV").extract[String])
    record.put("cardLast4Digits", (json \ "cardLast4Digits").extract[String])
    record.put("transactionType", (json \ "transactionType").extract[String])
    record.put("echoBuffer", (json \ "echoBuffer").extract[String])
    record.put("currentBalance", (json \ "currentBalance").extract[Float])
    record.put("merchantCity", (json \ "merchantCity").extract[String])
    record.put("merchantState", (json \ "merchantState").extract[String])
    record.put("merchantZip", (json \ "merchantZip").extract[String])
    record.put("cardPresent", (json \ "cardPresent").extract[Boolean])
    record.put("posOnPremises", (json \ "posOnPremises").extract[String])
    record.put("recurringAuthInd", (json \ "recurringAuthInd").extract[String])
    record.put("expirationDateKeyInMatch", (json \ "expirationDateKeyInMatch").extract[Boolean])
    record.put("isFraud", (json \ "isFraud").extract[Boolean])
    record.put("somenewRecord", (json \ "somenewRecord").extract[String])

    // Define Kafka producer within the partition scope to ensure serializability
    val producer = new KafkaProducer[String, GenericRecord](broadcastKafkaProperties.value)

    // Create a ProducerRecord with the Avro record
    val producerRecord = new ProducerRecord[String, GenericRecord]("test-topic1", null, record) // Use `null` for key if not needed

    // Send to Kafka
    producer.send(producerRecord, new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        if (exception != null) {
          println(s"Error sending message: ${exception.getMessage}")
        } else {
          println(s"Message sent to topic ${metadata.topic()} at partition ${metadata.partition()} with offset ${metadata.offset()}")
        }
      }
    })

    // Close the producer
    producer.close()

    // Stop the SparkSession
    spark.stop()
  }
}
/*
* {
     "name": "somenewRecord",
     "type": [
       "null",
       "string"
     ],
     "default": null
   }
*
*
* */
