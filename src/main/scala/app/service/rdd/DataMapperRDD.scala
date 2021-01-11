package app.service.rdd

import app.entity.{ExpediaData, HotelInfo}
import com.typesafe.config.Config
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{KafkaUtils, OffsetRange}
import org.json4s.jackson.JsonMethods.parse
import org.json4s.{DefaultFormats, JNull}

import scala.collection.JavaConverters.mapAsJavaMapConverter

class DataMapperRDD extends Serializable {

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

  def getDataFromHdfs(config: Config) = {
    val filePath = config.getString("hdfs.filePath")
    val sparkSession = SparkSession.builder().appName("HotelsBooking").getOrCreate()

    sparkSession.read
      .format("avro")
      .load(filePath).rdd.map(row => {
      ExpediaData(row.getLong(0), row.getString(1), row.getInt(2), row.getInt(3),
        row.getInt(4), row.getInt(5), row.getInt(6), row.getAs(7), row.getInt(8), row.getInt(9), row.getInt(10),
        row.getInt(11), row.getString(12), row.getString(13), row.getInt(14), row.getInt(15),
        row.getInt(16), row.getInt(17), row.getInt(18), row.getLong(19).toDouble)
    })
  }
}
