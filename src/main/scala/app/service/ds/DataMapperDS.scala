package app.service.ds

import app.entity.{ExpediaData, HotelInfo}
import app.service.df.DataMapperDF
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DoubleType

class DataMapperDS {

  val spark = SparkSession.builder
    .appName("HotelsBooking")
    .getOrCreate()

  def getDataFromKafka(config: Config) = {
    import spark.implicits._
    val inputDF = new DataMapperDF().getDataFromKafka(config)

    val transformedInputDF = inputDF
      .withColumn("Id_tmp", col("Id").cast(DoubleType))
      .drop(col("Id"))
      .withColumnRenamed("Id_tmp", "Id")

    transformedInputDF.as[HotelInfo]
  }

  def getDataFromHdfs(config: Config) = {
    import spark.implicits._

    val filePath = config.getString("hdfs.filePath")
    spark.read
      .format("avro")
      .option("inferSchema", "true")
      .load(filePath)
      .as[ExpediaData]
  }
}
