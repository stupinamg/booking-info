package app.service.rdd

import app.entity.{ExpediaData, HotelInfo}
import app.partitioner.SrchCiPartitioner
import com.typesafe.config.Config
import org.apache.spark.rdd.RDD

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

class DataProcessorRDD extends Serializable {

  def calculateIdleDays(rdd: RDD[ExpediaData]) = {
    rdd.filter(row => row.srch_co != null)
      .map(value => {
        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        val dateIn = LocalDate.parse(value.srch_ci, formatter)
        val dateOut = LocalDate.parse(value.srch_co, formatter)
        val idleDays = ChronoUnit.DAYS.between(dateIn, dateOut)
        (value, idleDays)
      })
  }

  def validateHotelsData(expediaData: RDD[(ExpediaData, Long)],
                         hotelsData: RDD[HotelInfo]) = {
    val preparedExpediaData = expediaData.map(row => (row._1.hotel_id, row))
    val preparedHotelsData = hotelsData.map(row => (row.id, row))
    val joinedBookingData = preparedHotelsData.join(preparedExpediaData)
    val invalidBookingData = joinedBookingData.filter(row => row._2._2._2 >= 2 && (row._2._2._2 < 30))
    invalidBookingData.take(5).foreach(f => println("Booking data with invalid rows: " + f))

    val validBookingData = joinedBookingData.subtract(invalidBookingData)
    val groupedByCountry = validBookingData.groupBy(_._2._1.country)
    groupedByCountry.take(5).foreach(f => println("Grouped by hotel county: " + f))

    val groupedByCity = validBookingData.groupBy(_._2._1.city)
    groupedByCity.take(5).foreach(f => println("Grouped by hotel city: " + f))
    expediaData.subtract(invalidBookingData.map(_._2._2))
  }

  def storeValidExpediaData(rdd: RDD[(ExpediaData, Long)], config: Config) = {
    val hdfsPath = config.getString("hdfs.validDataPath")
    val parentNumPartitions = rdd.partitions.length
    val transformedRdd = rdd.map(f => {
      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
      val year = LocalDate.parse(f._1.srch_ci, formatter).getYear
      (year, f._1)
    })

    transformedRdd
      .partitionBy(new SrchCiPartitioner(parentNumPartitions))
      .saveAsTextFile(hdfsPath)
  }
}
