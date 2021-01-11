package app.service.ds

import app.entity.{ExpediaData, HotelInfo, ValidExpediaData}
import app.service.df.DataProcessorDF
import com.typesafe.config.Config
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, SparkSession}

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

class DataProcessorDS {

  val spark = SparkSession.builder()
    .appName("HotelsBooking")
    .getOrCreate()

  def calculateIdleDays(ds: Dataset[ExpediaData]) = {
    import spark.implicits._

    val dataWithIdleDays = ds.filter(ds.col("srch_co").isNotNull).map(row => {
      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
      val dateIn = LocalDate.parse(row.srch_ci, formatter)
      val dateOut = LocalDate.parse(row.srch_co, formatter)
      val idleDays = ChronoUnit.DAYS.between(dateIn, dateOut)
      (row, idleDays)
    })

    dataWithIdleDays.map(row => {
      ValidExpediaData(row._1.id, row._1.date_time, row._1.site_name, row._1.posa_continent,
        row._1.user_location_country, row._1.user_location_region, row._1.user_location_city,
        row._1.orig_destination_distance, row._1.user_id, row._1.is_mobile, row._1.is_package, row._1.channel,
        row._1.srch_ci, row._1.srch_co, row._1.srch_adults_cnt, row._1.srch_children_cnt, row._1.srch_rm_cnt,
        row._1.srch_destination_id, row._1.srch_destination_type_id, row._1.hotel_id, row._2)
    })
  }

  def validateHotelsData(expediaData: Dataset[ValidExpediaData],
                         hotelsData: Dataset[HotelInfo]) = {
    val invalidData = expediaData.filter(row => row.idle_days >= 2 && row.idle_days < 30)
    invalidData.take(5).foreach(f => println("Booking data with invalid rows: " + f))

    val joinedInvalidData = hotelsData
      .join(invalidData, hotelsData.col("id") === invalidData.col("hotel_id"))
    joinedInvalidData.take(5).foreach(f => println("Hotel data for invalid rows: " + f))

    val validData = expediaData.except(invalidData)
    val joinedValidData = validData
      .join(hotelsData, validData.col("hotel_id" ) === hotelsData.col("id"))

    val groupedByCountry = joinedValidData.groupBy(col("Country")).count()
    groupedByCountry.take(5).foreach(f => println("Grouped by hotel county: " + f))
    val groupedByCity = joinedValidData.groupBy(col("City")).count()
    groupedByCity.take(5).foreach(f => println("Grouped by hotel city: " + f))

    validData
  }

  def storeValidExpediaData(ds: Dataset[ValidExpediaData], config: Config) = {
    new DataProcessorDF().storeValidExpediaData(ds.toDF(), config)
  }
}
