package app

import app.service.rdd.{DataMapperRdd, DataProcessorRdd}
import app.service.df.{DataMapperDf, DataProcessorDf}
import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}

object Main {

  val config = ConfigFactory.load(s"resources/application.conf")

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("HotelsBooking")
      .setMaster("local[*]")
      .set("spark.streaming.kafka.consumer.cache.enabled", "false")
      .set("spark.streaming.kafka.consumer.poll.ms", "512")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerialize")
      .registerKryoClasses(Array(classOf[String]))

    val sc = new SparkContext(conf)
    val dataProcessorRdd = new DataProcessorRdd()
    val dataMapperRdd = new DataMapperRdd()
    val dataMapperDf = new DataMapperDf()
    val dataProcessorDf = new DataProcessorDf()

    // rdd option
//    val hotelsRdd = dataMapperRdd.getDataFromKafka(sc, config)
//    val expediaRdd = dataMapperRdd.getDataFromHdfs(config)
//    val idleRdd = dataProcessorRdd.calculateIdleDays(expediaRdd)
//    val data = dataProcessorRdd.validateHotelsData(idleRdd, hotelsRdd)
//    dataProcessorRdd.storeValidExpediaData(data, config)

    // df option
    val hotelsDf = dataMapperDf.getDataFromKafkaDf(config)
    val expediaDf = dataMapperDf.getDataFromHdfsDf(config)
    val idleDf = dataProcessorDf.calculateIdleDaysDf(expediaDf)
    val data = dataProcessorDf.validateHotelsDataDf(idleDf, hotelsDf)
    dataProcessorDf.storeValidExpediaData(data, config)

    sc.stop()
  }

}
