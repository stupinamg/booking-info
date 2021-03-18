package app.service.processor

import app.entity.{ExpediaData, HotelInfo, ValidExpediaData}
import app.service.traits.SparkSessionTestWrapper
import com.typesafe.config.ConfigFactory
import org.scalatest.FlatSpec

class DataProcessorRDDTestSpec extends FlatSpec with SparkSessionTestWrapper {

  val processor = new DataProcessorRDD
  implicit val testConfig = ConfigFactory.load(s"resources/testApplication.conf")

  "The DataProcessorDF" should "calculate idle days correct" in {
    val expediaData = sparkContext.parallelize(Seq(
      ExpediaData(428944, "2015-04-25 01:29:19", 8, 4, 77, 977, 45054, Option(10455.265), 224521, 0, 0, 0,
        "2016-10-21", "2016-10-24", 3, 0, 1, 8745, 1, 2843268349952L)
    ))

    val result = processor.calculateIdleDays(expediaData)
    val idleDays = result.map(row => row.idle_days).collect()
    assert(idleDays(0) == 3)
  }

  it should "validate booking data correct" in {
    val expediaData = sparkContext.parallelize(Seq(
      ValidExpediaData(402108, "2015-01-25 11:38:08", 2, 3, 66, 258, 10501, 68.8369, 135709, 0, 0, 0,
        "2017-09-14", "2017-09-15", 2, 1, 1, 1400, 1, 1365799600130L, 1),
      ValidExpediaData(428944, "2015-04-25 01:29:19", 8, 4, 77, 977, 45054, 10455.265, 224521, 0, 0, 0,
        "2016-10-21", "2016-10-24", 3, 0, 1, 8745, 1, 2843268349952L, 3)
    ))

    val hotelsData = sparkContext.parallelize(Seq(
      HotelInfo(1365799600130L, "Axel Hotel Barcelona Urban Spa Adults Only", "ES", "Barcelona",
        "Aribau 33 Eixample 08011 Barcelona Spain", "41.3873478", "2.1603987", "sp3e")
    ))

    val result = processor.validateHotelsData(expediaData, hotelsData)
    assert(result.count() == 1)
  }

  it should "store booking data partitioned by year" in {
    val expediaData = sparkContext.parallelize(Seq(
      ValidExpediaData(402108, "2015-01-25 11:38:08", 2, 3, 66, 258, 10501, 68.8369, 135709, 0, 0, 0,
        "2017-09-14", "2017-09-15", 2, 1, 1, 1400, 1, 1365799600130L, 1),
      ValidExpediaData(428944, "2015-04-25 01:29:19", 8, 4, 77, 977, 45054, 10455.265, 224521, 0, 0, 0,
        "2016-10-21", "2016-10-24", 3, 0, 1, 8745, 1, 2843268349952L, 1)
    ))

    processor.storeValidExpediaData(expediaData, testConfig)
    val pathName = "src/test/scala/resources/valid-expedia-data"
    val files = new java.io.File(pathName).list()

    assert(files.length != 0)
  }

}
