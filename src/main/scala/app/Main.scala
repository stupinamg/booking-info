package app

import app.service.df.{DataMapperDF, DataProcessorDF}
import app.service.ds.{DataMapperDS, DataProcessorDS}
import app.service.rdd.{DataMapperRDD, DataProcessorRDD}
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
    val hotelsDF = dataMapperDF.getDataFromKafkaDf(config)
    val expediaDF = dataMapperDF.getDataFromHdfsDf(config)
    val idleDF = dataProcessorDF.calculateIdleDaysDf(expediaDF)
    val dataDF = dataProcessorDF.validateHotelsDataDf(idleDF, hotelsDF)
    dataProcessorDF.storeValidExpediaData(dataDF, config)

    // Dataset option
    //    val hotelsDS = dataMapperDS.getDataFromKafkaDs(config)
    //    val expediaDS = dataMapperDS.getDataFromHdfsDs(config)
    //    val idleDS = dataProcessorDS.calculateIdleDaysDf(expediaDS)
    //    val dataDS = dataProcessorDS.validateHotelsDataDf(idleDS, hotelsDS)
    //    dataProcessorDf.storeValidExpediaData(dataDS, config)

    sc.stop()
  }

}
