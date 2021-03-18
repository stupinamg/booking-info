package app.service.processor

import app.entity.{ExpediaData, HotelInfo, ValidExpediaData}
import app.service.traits.SparkSessionTestWrapper
import com.typesafe.config.ConfigFactory
import org.scalatest.FlatSpec

class DataProcessorDSTestSpec extends FlatSpec with SparkSessionTestWrapper {

  import spark.implicits._

  val processor = new DataProcessorDS
  val testConfig = ConfigFactory.load(s"resources/testApplication.conf")

  "The DataProcessorDF" should "calculate idle days correct" in {
    val expediaData = Seq(
      ExpediaData(428944, "2015-04-25 01:29:19", 8, 4, 77, 977, 45054, Option(10455.265), 224521, 0, 0, 0,
        "2016-10-21", "2016-10-24", 3, 0, 1, 8745, 1, 2843268349952L)
    ).toDS()

    val result = processor.calculateIdleDays(expediaData)
    val idleDays = result.select("idle_days").as[Long].collect()
    assert(idleDays(0) == 3)
  }

  it should "store booking data partitioned by year" in {
    val expediaData = Seq(
      ValidExpediaData(402108, "2015-01-25 11:38:08", 2, 3, 66, 258, 10501, 68.8369, 135709, 0, 0, 0,
        "2017-09-14", "2017-09-15", 2, 1, 1, 1400, 1, 1365799600130L, 1)
    ).toDS()

    processor.storeValidExpediaData(expediaData, testConfig)
    val pathName = "src/test/scala/resources/valid-expedia-data/year=2017"
    val files = new java.io.File(pathName).list()

    assert(files.length != 0)
  }

}
