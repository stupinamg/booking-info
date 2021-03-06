package app

import app.service.mapper.{DataMapperDF, DataMapperDS, DataMapperRDD}
import app.service.processor.{DataProcessorDF, DataProcessorDS, DataProcessorRDD}
import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}

/** Application entry point */
object Main {

  val config = ConfigFactory.load(s"resources/application.conf")

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("HotelsBooking")
      .setMaster("local[*]")
      .set("spark.streaming.kafka.consumer.cache.enabled", "false")
      .set("spark.streaming.kafka.consumer.poll.ms", "512")
      .set("max.poll.interval.ms", "1000")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.allowMultipleContexts", "true")
      .registerKryoClasses(Array(classOf[String]))

    val sc = new SparkContext(conf)
    val dataMapperRDD = new DataMapperRDD()
    val dataMapperDF = new DataMapperDF()
    val dataMapperDS = new DataMapperDS()

    val dataProcessorRDD = new DataProcessorRDD()
    val dataProcessorDF = new DataProcessorDF()
    val dataProcessorDS = new DataProcessorDS()

    // RDD option
    val hotelsRDD = dataMapperRDD.getDataFromKafka(sc, config)
    val expediaRDD = dataMapperRDD.getDataFromHdfs(config)
    val idleRDD = dataProcessorRDD.calculateIdleDays(expediaRDD)
    val dataRDD = dataProcessorRDD.validateHotelsData(idleRDD, hotelsRDD)
    dataProcessorRDD.storeValidExpediaData(dataRDD, config)

    // DF option
    val hotelsDF = dataMapperDF.getDataFromKafka(config)
    val expediaDF = dataMapperDF.getDataFromHdfs(config)
    val idleDF = dataProcessorDF.calculateIdleDays(expediaDF)
    val dataDF = dataProcessorDF.validateHotelsData(idleDF, hotelsDF)
    dataProcessorDF.storeValidExpediaData(dataDF, config)

    // Dataset option
    val hotelsDS = dataMapperDS.getDataFromKafka(config)
    val expediaDS = dataMapperDS.getDataFromHdfs(config)
    val idleDS = dataProcessorDS.calculateIdleDays(expediaDS)
    val dataDS = dataProcessorDS.validateHotelsData(idleDS, hotelsDS)
    dataProcessorDS.storeValidExpediaData(dataDS, config)

    sc.stop()
  }
}
