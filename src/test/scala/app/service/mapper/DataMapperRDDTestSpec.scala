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
    val kafkaTopic = testConfig.getString("kafka.topics")
    val kafkaBroker = testConfig.getString("kafka.broker")
    val kafkaStartOffset = testConfig.getString("kafka.startingOffset")
    val kafkaEndOffset = testConfig.getString("kafka.endingOffset")

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
