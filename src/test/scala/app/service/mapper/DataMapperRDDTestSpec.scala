package app.service.mapper

import app.service.traits.SparkSessionTestWrapper
import com.typesafe.config.ConfigFactory
import net.manub.embeddedkafka.Codecs.stringSerializer
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.{FlatSpec, Matchers}

class DataMapperRDDTestSpec extends FlatSpec with Matchers
  with EmbeddedKafka with SparkSessionTestWrapper {

  val topic = "hotels-data2"
  val consumer = new DataMapperRDD
  implicit val config = EmbeddedKafkaConfig(kafkaPort = 9093, zooKeeperPort = 2182)
  implicit val testConfig = ConfigFactory.load(s"resources/testApplication.conf")

  "Config for Kafka" should "be read successfully" in {
    val confPath = s"resources/testApplication.conf"
    val kafkaTopic = "hotels-data2"
    val kafkaBroker = "localhost:9093"
    val kafkaStartOffset = "0"
    val kafkaEndOffset = "200000"

    assert(kafkaTopic == "hotels-data2")
    assert(kafkaBroker == "localhost:9093")
    assert(kafkaStartOffset == "0")
    assert(kafkaEndOffset == "200000")
  }

  it should "read data from HDFS" in {
    val data = consumer.getDataFromHdfs(testConfig)
    assert(data.count() == 1)
  }

}
