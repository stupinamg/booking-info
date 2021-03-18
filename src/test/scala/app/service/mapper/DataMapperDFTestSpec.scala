package app.service.mapper

import app.service.traits.SparkSessionTestWrapper
import com.typesafe.config.ConfigFactory
import net.manub.embeddedkafka.Codecs.stringSerializer
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest._

class DataMapperDFTestSpec extends FlatSpec with Matchers
  with EmbeddedKafka with SparkSessionTestWrapper {

  val topic = "hotels-data2"
  val consumer = new DataMapperDF
  implicit val config = EmbeddedKafkaConfig(kafkaPort = 9093, zooKeeperPort = 2182)
  implicit val testConfig = ConfigFactory.load(s"resources/testApplication.conf")

  "Config for Kafka" should "be read successfully" in {
    val confPath = s"resources/testApplication.conf"
    val kafkaTopic = ConfigFactory.load(confPath).getConfig("kafka").getString("topics")
    val kafkaBroker = ConfigFactory.load(confPath).getConfig("kafka").getString("broker")
    val kafkaStartOffset = ConfigFactory.load(confPath).getConfig("kafka").getString("startOffset")
    val kafkaEndOffset = ConfigFactory.load(confPath).getConfig("kafka").getString("endOffset")

    assert(kafkaTopic == "hotels-data2")
    assert(kafkaBroker == "localhost:9093")
    assert(kafkaStartOffset == """{"hotels-data2":{"0":-2}}""")
    assert(kafkaEndOffset == """{"hotels-data2":{"0":-1}}""")
  }

  it should "read data from HDFS" in {
    val data = consumer.getDataFromHdfs(testConfig)
    assert(data.count() == 1)
  }
}



