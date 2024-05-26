package com.its.esales.framework.jobs

import org.apache.spark.sql.SparkSession
import com.its.esales.streaming.kafka.consumer.CustomerS3Ingestor
import com.its.esales.batch.productinfo.ProductStoreManual
object JobFactory {
  def createJob(spark: SparkSession, jobType: JobEnum.JobEnum): Job = {
    jobType match {
      case JobEnum.CustomerS3Ingestor => new CustomerS3Ingestor(spark)
      case JobEnum.ProductStoreManual => new ProductStoreManual()


      //      case JobEnum.OtherJob =>
      //        new  OtherJob(spark)
      // Add more cases for additional job types

      case _ =>
        throw new IllegalArgumentException("Invalid job type")
    }
  }

}
