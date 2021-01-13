package app.service.processor

import app.service.traits.SparkSessionTestWrapper
import com.typesafe.config.ConfigFactory
import org.scalatest.FlatSpec

class DataProcessorDFTestSpec extends FlatSpec with SparkSessionTestWrapper {

  import spark.implicits._

  val processor = new DataProcessorDF
  val testConfig = ConfigFactory.load(s"resources/testApplication.conf")

  "The DataProcessorDF" should "calculate idle days correct" in {
    val expediaData = Seq(
      (428944, "2015-04-25 01:29:19", 8, 4, 77, 977, 45054, 10455.265, 224521, 0, 0, 0, "2016-10-21",
        "2016-10-24", 3, 0, 1, 8745, 1, 2843268349952L)
    ).toDF("id", "date_time", "site_name", "posa_continent", "user_location_country",
      "user_location_region", "user_location_city", "orig_destination_distance", "user_id", "is_mobile",
      "is_package", "channel", "srch_ci", "srch_co", "srch_adults_cnt", "srch_children_cnt",
      "srch_rm_cnt", "srch_destination_id", "srch_destination_type_id", "hotel_id")

    val result = processor.calculateIdleDays(expediaData)
    val idleDays = result.select("idle_days").as[Long].collect()
    assert(idleDays(0) == 3)
  }

  it should "validate booking data correct" in {
    val expediaData = Seq(
      (402108, "2015-01-25 11:38:08", 2, 3, 66, 258, 10501, 68.8369, 135709, 0, 0, 0, "2017-09-14",
        "2017-09-15", 2, 1, 1, 1400, 1, 1365799600130L, 1),
      (428944, "2015-04-25 01:29:19", 8, 4, 77, 977, 45054, 10455.265, 224521, 0, 0, 0, "2016-10-21",
        "2016-10-24", 3, 0, 1, 8745, 1, 2843268349952L, 3)
    ).toDF("id", "date_time", "site_name", "posa_continent", "user_location_country",
      "user_location_region", "user_location_city", "orig_destination_distance", "user_id", "is_mobile",
      "is_package", "channel", "srch_ci", "srch_co", "srch_adults_cnt", "srch_children_cnt",
      "srch_rm_cnt", "srch_destination_id", "srch_destination_type_id", "hotel_id", "idle_days")

    val hotelsData = Seq(
      (1365799600130L, "Axel Hotel Barcelona Urban Spa Adults Only", "ES", "Barcelona",
        "Aribau 33 Eixample 08011 Barcelona Spain", "41.3873478", "2.1603987", "sp3e")
    ).toDF("id", "name", "country", "city", "address", "lalitude", "longitude", "geohash")

    val result = processor.validateHotelsData(expediaData, hotelsData)
    assert(result.count() == 1)
  }

  it should "store booking data partitioned by year" in {
    val expediaData = Seq(
      (402108, "2015-01-25 11:38:08", 2, 3, 66, 258, 10501, 68.8369, 135709, 0, 0, 0, "2017-09-14",
        "2017-09-15", 2, 1, 1, 1400, 1, 1365799600130L, 1)
    ).toDF("id", "date_time", "site_name", "posa_continent", "user_location_country",
      "user_location_region", "user_location_city", "orig_destination_distance", "user_id", "is_mobile",
      "is_package", "channel", "srch_ci", "srch_co", "srch_adults_cnt", "srch_children_cnt",
      "srch_rm_cnt", "srch_destination_id", "srch_destination_type_id", "hotel_id", "idle_days")

    processor.storeValidExpediaData(expediaData, testConfig)
    val pathName = "src/test/scala/resources/valid-expedia-data/year=2017"
    val files = new java.io.File(pathName).list()

    assert(files.length != 0)
  }

}