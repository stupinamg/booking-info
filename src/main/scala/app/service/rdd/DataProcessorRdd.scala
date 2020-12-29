package app.service.rdd

import app.entity.{ExpediaData, HotelInfo}
import app.partitioner.SrchCiPartitioner
import com.typesafe.config.Config
import org.apache.spark.rdd.RDD

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

class DataProcessorRdd extends Serializable {

  def calculateIdleDays(rdd: RDD[(Double, ExpediaData)]): RDD[(Double, (ExpediaData, Long))] = {
    rdd.filter(row => row._2.srchCo != null)
      .map(value => {
        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        val dateIn = LocalDate.parse(value._2.srchCi, formatter)
        val dateOut = LocalDate.parse(value._2.srchCo, formatter)
        val idleDays = ChronoUnit.DAYS.between(dateIn, dateOut)
        (value._1, (value._2, idleDays))
      })
  }

  def validateHotelsData(expediaData: RDD[(Double, (ExpediaData, Long))],
                         hotelsData: RDD[(Double, HotelInfo)]) = {
    val joinedBookingData = hotelsData.join(expediaData)
    val invalidBookingData = joinedBookingData.filter(row => row._2._2._2.>=(2).&&(row._2._2._2 < 30))

    invalidBookingData.take(5).foreach(f => println("Booking data with invalid rows: " + f))

    val validBookingData = joinedBookingData.subtract(invalidBookingData)

    val groupedByCountry = validBookingData.groupBy(_._2._1.country)
    groupedByCountry.take(5).foreach(f => println("Grouped by hotel county: " + f))

    val groupedByCity = validBookingData.groupBy(_._2._1.city)
    groupedByCity.take(5).foreach(f => println("Grouped by hotel city: " + f))

    val validExpediaData = expediaData.subtract(invalidBookingData.map(f => (f._1, f._2._2)))
    validExpediaData
  }

  def storeValidExpediaData(rdd: RDD[(Double, (ExpediaData, Long))], config: Config): Unit = {
    val hdfsPath = config.getString("hdfs.hdfsPath")
    val transformedRdd = rdd.map(f => (f._2._1, f._1))
    val parentNumPartitions = rdd.partitions.length
    transformedRdd.partitionBy(new SrchCiPartitioner(parentNumPartitions)).saveAsTextFile(hdfsPath)
  }
}
