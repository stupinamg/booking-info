package app.service.mapper

import app.entity.{ExpediaData, HotelInfo}
import com.typesafe.config.Config
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{Encoders, SparkSession}

/** Dataset mapper for the data obtained from different sources */
class DataMapperDS {

  val spark = SparkSession.builder
    .appName("HotelsBooking")
    .getOrCreate()

  /** Reads data from Kafka
   *
   * @param config configuration values for the Kafka
   * @return dataset of the hotels data
   */
  def getDataFromKafka(config: Config) = {
    import spark.implicits._
    val inputDF = new DataMapperDF().getDataFromKafka(config)

    val transformedInputDF = inputDF
      .withColumn("Id_tmp", col("Id").cast(DoubleType))
      .drop(col("Id"))
      .withColumnRenamed("Id_tmp", "Id")

    transformedInputDF.as[HotelInfo]
  }

  /** Reads data from HDFS
   *
   * @param config configuration values for the HDFS
   * @return dataset of the expedia data
   */
  def getDataFromHdfs(config: Config) = {
    import spark.implicits._
    val schema = Encoders.product[ExpediaData].schema
    val filePath = config.getString("hdfs.filePath")

    spark.read
      .format("avro")
      .schema(schema)
      .load(filePath)
      .as[ExpediaData]
  }
}
