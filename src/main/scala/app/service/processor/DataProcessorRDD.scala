package app.service.processor

import app.entity.{ExpediaData, HotelInfo, ValidExpediaData}
import app.partitioner.YearPartitioner
import com.typesafe.config.Config
import org.apache.spark.rdd.RDD

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

/** Processes data in RDD */
class DataProcessorRDD extends Serializable {

  val MIN_DAYS = 2
  val MAX_DAYS = 30

  /** Calculates dates between check-in and check-out
   *
   * @param rdd expedia data
   * @return RDD of expedia data with idle days
   */
  def calculateIdleDays(rdd: RDD[ExpediaData]) = {
    rdd.filter(row => row.srch_co != null)
      .map(row => {
        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        val dateIn = LocalDate.parse(row.srch_ci, formatter)
        val dateOut = LocalDate.parse(row.srch_co, formatter)
        val idleDays = ChronoUnit.DAYS.between(dateIn, dateOut)

        ValidExpediaData(row.id, row.date_time, row.site_name, row.posa_continent,
          row.user_location_country, row.user_location_region, row.user_location_city,
          row.orig_destination_distance.getOrElse(0), row.user_id, row.is_mobile,
          row.is_package, row.channel, row.srch_ci, row.srch_co, row.srch_adults_cnt,
          row.srch_children_cnt, row.srch_rm_cnt, row.srch_destination_id,
          row.srch_destination_type_id, row.hotel_id.toDouble, idleDays)
      })
  }

  /** Validates booking data
   *
   * @param expediaData
   * @param hotelsData
   * @return RDD of valid booking data
   */
  def validateHotelsData(expediaData: RDD[ValidExpediaData],
                         hotelsData: RDD[HotelInfo]) = {
    val invalidBookingData = expediaData.filter(row => row.idle_days >= MIN_DAYS
      && row.idle_days < MAX_DAYS)

    val preparedExpediaData = expediaData.map(value => (value.hotel_id, value))
    val preparedHotelsData = hotelsData.map(value => (value.id, value))
    val joinedInvalidData = preparedHotelsData.join(preparedExpediaData)
    // print info for invalid rows
    joinedInvalidData.take(50).foreach(f => println("Booking data for invalid rows: " +
      "hotel_id: " + f._2._1.id +
      "hotel_name: " + f._2._1.name +
      "hotel_country: " + f._2._1.country +
      "hotel_city: " + f._2._1.city +
      "invalid_idle_days: " + f._2._2.idle_days))

    val validBookingData = expediaData.subtract(invalidBookingData)
    val joinedValidData = validBookingData.map(value => (value.hotel_id, value)).join(preparedHotelsData)

    //booking data grouped by country
    val groupedByCountry = joinedValidData.groupBy(_._2._2.country)
    groupedByCountry.take(50).foreach(f => println("Grouped hotel info by country: " + f))

    //booking data grouped by city
    val groupedByCity = joinedValidData.groupBy(_._2._2.city)
    groupedByCity.take(50).foreach(f => println("Grouped hotel info by city: " + f))
    validBookingData
  }

  /** Stores valid expedia data to HDFS
   *
   * @param rdd    valid expedia data
   * @param config configuration values for the HDFS
   */
  def storeValidExpediaData(rdd: RDD[ValidExpediaData], config: Config) = {
    val hdfsPath = "src/test/scala/resources/valid-expedia-data"

    val pairedRDD = rdd.map(f => {
      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
      val year = LocalDate.parse(f.srch_ci, formatter).getYear
      (year, f)
    })
    val numPartitions = pairedRDD.groupByKey().count().toInt

    pairedRDD
      .partitionBy(new YearPartitioner(numPartitions))
      .saveAsTextFile(hdfsPath)
  }
}
