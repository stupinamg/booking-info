package app.service.df

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}

class DataMapperDF {

  def getDataFromKafkaDf(config: Config)= {
    val spark = SparkSession.builder
      .appName("HotelsBooking")
      .getOrCreate()

    import spark.implicits._
    val topic = config.getString("kafka.topics").split(",").toSet.head
    val starting = config.getString("kafka.startOffset")
    val ending = config.getString("kafka.endOffset")

    val inputDf = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", config.getString("kafka.broker"))
      .option("subscribe", topic)
      .option("startingOffsets", starting)
      .option("endingOffsets", ending)
      .option("failOnDataLoss", "false")
      .load()
      .selectExpr("CAST(value AS STRING) as string").as[String]
   spark.read.json(inputDf)
  }

  def getDataFromHdfsDf(config: Config) = {
    val filePath = config.getString("hdfs.filePath")
    val spark = SparkSession.builder()
      .appName("HotelsBooking")
      .getOrCreate()

    spark.read
      .format("avro")
      .load(filePath)
  }
}
