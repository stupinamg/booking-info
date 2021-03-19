package app.service.processor

import app.entity.{ExpediaData, HotelInfo, ValidExpediaData}
import com.typesafe.config.Config
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, SparkSession}

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

/** Processes data in dataset */
class DataProcessorDS extends Serializable {

  val spark = SparkSession.builder()
    .appName("HotelsBooking")
    .getOrCreate()

  val MIN_DAYS = 2
  val MAX_DAYS = 30

  /** Calculates dates between check-in and check-out
   *
   * @param ds expedia data
   * @return dataset of expedia data with idle days
   */
  def calculateIdleDays(ds: Dataset[ExpediaData]) = {
    import spark.implicits._

    ds.filter(ds.col("srch_co").isNotNull).map(row => {
      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
      val dateIn = LocalDate.parse(row.srch_ci, formatter)
      val dateOut = LocalDate.parse(row.srch_co, formatter)
      val idleDays = ChronoUnit.DAYS.between(dateIn, dateOut)

      ValidExpediaData(row.id, row.date_time, row.site_name, row.posa_continent,
        row.user_location_country, row.user_location_region, row.user_location_city,
        row.orig_destination_distance.getOrElse(0), row.user_id, row.is_mobile, row.is_package, row.channel,
        row.srch_ci, row.srch_co, row.srch_adults_cnt, row.srch_children_cnt, row.srch_rm_cnt,
        row.srch_destination_id, row.srch_destination_type_id, row.hotel_id.toDouble, idleDays)
    })
  }

  /** Validates booking data
   *
   * @param expediaData
   * @param hotelsData
   * @return dataset of valid booking data
   */
  def validateHotelsData(expediaData: Dataset[ValidExpediaData],
                         hotelsData: Dataset[HotelInfo]) = {
    val invalidData = expediaData.filter(row => row.idle_days >= MIN_DAYS
      && row.idle_days < MAX_DAYS)

    // show info for invalid rows
    hotelsData
      .join(invalidData, hotelsData.col("id") === invalidData.col("hotel_id"))
      .show(50)

    val validData = expediaData.except(invalidData)
    val joinedValidData = validData
      .join(hotelsData, validData.col("hotel_id") === hotelsData.col("id"))

    //booking data grouped by country
    joinedValidData.groupBy(col("Country"))
      .count()
      .show(50)

    //booking data grouped by city
    joinedValidData.groupBy(col("City"))
      .count()
      .show(50)
    validData
  }

  /** Stores valid expedia data to HDFS
   *
   * @param ds     valid expedia data
   * @param config configuration values for the HDFS
   */
  def storeValidExpediaData(ds: Dataset[ValidExpediaData], config: Config) = {
    new DataProcessorDF().storeValidExpediaData(ds.toDF(), config)
  }
}
