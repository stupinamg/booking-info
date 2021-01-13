package app.service.mapper

import app.entity.{ExpediaData, HotelInfo}
import com.typesafe.config.Config
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{KafkaUtils, OffsetRange}
import org.json4s.jackson.JsonMethods.parse
import org.json4s.{DefaultFormats, JNull}

import scala.collection.JavaConverters.mapAsJavaMapConverter

/** RDD mapper for the data obtained from different sources */
class DataMapperRDD extends Serializable {

  /** Reads data from Kafka
   *
   * @param sc     spark context
   * @param config configuration values for the Kafka
   * @return RDD of the hotels data
   */
  def getDataFromKafka(sc: SparkContext, config: Config) = {
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> config.getString("kafka.broker"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "hotels",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> "false"
    ).asJava

    val topic = config.getString("kafka.topics").split(",").toSet.head
    val startingOffset = config.getString("kafka.startingOffset").toInt
    val endingOffset = config.getString("kafka.endingOffset").toInt
    val offsetRanges = Array(OffsetRange(topic, 0, startingOffset, endingOffset))

    val rdd = KafkaUtils.createRDD[String, String](sc, kafkaParams, offsetRanges, PreferConsistent)
    rdd.map(_.value()).map(jsonString => {
      implicit val formats = DefaultFormats
      val parsedJson = parse(jsonString)
      HotelInfo((parsedJson \ "Id").extract[String].toDouble, (parsedJson \ "Name").extract[String],
        (parsedJson \ "Country").extract[String], (parsedJson \ "City").extract[String],
        (parsedJson \ "Address").extractOrElse(JNull.values), (parsedJson \ "Latitude").extract[String],
        (parsedJson \ "Longitude").extract[String], (parsedJson \ "Geohash").extract[String])
    })
  }

  /** Reads data from HDFS
   *
   * @param config configuration values for the HDFS
   * @return RDD of the expedia data
   */
  def getDataFromHdfs(config: Config) = {
    val filePath = config.getString("hdfs.filePath")
    val spark = SparkSession.builder()
      .appName("HotelsBooking")
      .getOrCreate()

    import spark.implicits._
    val schema = Encoders.product[ExpediaData].schema

    spark.read
      .format("avro")
      .schema(schema)
      .load(filePath)
      .as[ExpediaData].rdd
  }
}
