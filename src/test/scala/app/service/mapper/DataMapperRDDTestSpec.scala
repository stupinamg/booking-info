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
    val kafkaTopic = ConfigFactory.load(confPath).getConfig("kafka").getString("topics")
    val kafkaBroker = ConfigFactory.load(confPath).getConfig("kafka").getString("broker")
    val kafkaStartOffset = ConfigFactory.load(confPath).getConfig("kafka").getString("startingOffset")
    val kafkaEndOffset = ConfigFactory.load(confPath).getConfig("kafka").getString("endingOffset")

    assert(kafkaTopic == "hotels-data2")
    assert(kafkaBroker == "localhost:9093")
    assert(kafkaStartOffset == "0")
    assert(kafkaEndOffset == "200000")
  }

  it should "consume message from published topic" in {
    EmbeddedKafka.start()

    val list = List(
      "{\"Id\":\"3.33289E+12\",\"Name\":\"Axel Hotel Barcelona Urban Spa Adults Only\",\"Country\":\"ES\"," +
        "\"City\":\"Barcelona\",\"Address\":\"Aribau 33 Eixample 08011 Barcelona Spain\"," +
        "\"Latitude\":\"41.3873478\",\"Longitude\":\"2.1603987\",\"Geohash\":\"sp3e\"}",
      "{\"Id\":\"3.34148E+12\",\"Name\":\"The Cleveland\",\"Country\":\"GB\",\"City\":\"London\"," +
        "\"Address\":\"39 40 Cleveland Square Westminster Borough London W2 6DA United Kingdom\"," +
        "\"Latitude\":\"51.5139692\",\"Longitude\":\"-0.1828202\",\"Geohash\":\"gcpv\"}"
    )
    list.foreach(message => publishToKafka(topic, message))
    val response = consumer.getDataFromKafka(sparkContext, testConfig)
    assert(response.count() == 2)

    EmbeddedKafka.stop()
  }

  it should "read data from HDFS" in {
    val data = consumer.getDataFromHdfs(testConfig)
    assert(data.count() == 101120)
  }

}