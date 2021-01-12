package app.service.processor

import app.entity.{ExpediaData, HotelInfo, ValidExpediaData}
import app.partitioner.YearPartitioner
import com.typesafe.config.Config
import org.apache.spark.rdd.RDD

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

class DataProcessorRDD extends Serializable {

  def calculateIdleDays(rdd: RDD[ExpediaData]) = {
    val dataWithIdleDays = rdd.filter(row => row.srch_co != null)
      .map(value => {
        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        val dateIn = LocalDate.parse(value.srch_ci, formatter)
        val dateOut = LocalDate.parse(value.srch_co, formatter)
        val idleDays = ChronoUnit.DAYS.between(dateIn, dateOut)
        (value, idleDays)
      })

    dataWithIdleDays.map(row => {
      ValidExpediaData(row._1.id, row._1.date_time, row._1.site_name, row._1.posa_continent,
        row._1.user_location_country, row._1.user_location_region, row._1.user_location_city,
        row._1.orig_destination_distance, row._1.user_id, row._1.is_mobile, row._1.is_package, row._1.channel,
        row._1.srch_ci, row._1.srch_co, row._1.srch_adults_cnt, row._1.srch_children_cnt, row._1.srch_rm_cnt,
        row._1.srch_destination_id, row._1.srch_destination_type_id, row._1.hotel_id, row._2)
    })
  }

  def validateHotelsData(expediaData: RDD[ValidExpediaData],
                         hotelsData: RDD[HotelInfo]) = {
    val invalidBookingData = expediaData.filter(row => row.idle_days >= 2 && row.idle_days < 30)
    invalidBookingData.take(5).foreach(f => println("Booking data with invalid rows: " + f))

    val preparedExpediaData = expediaData.map(value => (value.hotel_id, value))
    val preparedHotelsData = hotelsData.map(value => (value.id, value))
    val joinedInvalidData = preparedHotelsData.join(preparedExpediaData)
    joinedInvalidData.take(5).foreach(f => println("Hotel data for invalid rows: " + f))

    val validBookingData = expediaData.subtract(invalidBookingData)
    val joinedValidData = validBookingData.map(value => (value.hotel_id, value)).join(preparedHotelsData)

    val groupedByCountry = joinedValidData.groupBy(_._2._2.country)
    groupedByCountry.take(5).foreach(f => println("Grouped by hotel county: " + f))

    val groupedByCity = joinedValidData.groupBy(_._2._2.city)
    groupedByCity.take(5).foreach(f => println("Grouped by hotel city: " + f))
    validBookingData
  }

  def storeValidExpediaData(rdd: RDD[ValidExpediaData], config: Config) = {
    val hdfsPath = config.getString("hdfs.validDataPath")

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
