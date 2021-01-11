package app.service.df

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

class DataMapperDF {

  val spark = SparkSession.builder
    .appName("HotelsBooking")
    .getOrCreate()

  def getDataFromKafka(config: Config) = {
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

  def getDataFromHdfs(config: Config) = {
    val filePath = config.getString("hdfs.filePath")

    spark.read
      .format("avro")
      .load(filePath)
  }
}
