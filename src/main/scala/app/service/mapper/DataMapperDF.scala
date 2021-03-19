package app.service.mapper

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

/** Dataframe mapper for the data obtained from different sources */
class DataMapperDF {

  val spark = SparkSession.builder
    .appName("HotelsBooking")
    .getOrCreate()

  /** Reads data from Kafka
   *
   * @param config configuration values for the Kafka
   * @return dataframe of the hotels data
   */
  def getDataFromKafka(config: Config) = {
    import spark.implicits._

    val inputDf = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9093")
      .option("subscribe", "hotels-data2")
      .option("startingOffsets", "0")
      .option("endingOffsets", "200000")
      .option("failOnDataLoss", "false")
      .load()
      .selectExpr("CAST(value AS STRING) as string").as[String]
    spark.read.json(inputDf)
  }

  /** Reads data from HDFS
   *
   * @param config configuration values for the HDFS
   * @return dataframe of the expedia data
   */
  def getDataFromHdfs(config: Config) = {
    val filePath = "src/test/scala/resources/part-00000-expedia-data.avro"

    spark.read
      .format("avro")
      .load(filePath)
  }
}
