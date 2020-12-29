package app.service.df

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, date_format, udf}

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

class DataProcessorDf {

  def calculateIdleDaysDf(df: DataFrame): DataFrame = {
    val dtFunc = (arg1: String, arg2: String) => {
      val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
      val dateIn = LocalDate.parse(arg1, formatter)
      val dateOut = LocalDate.parse(arg2, formatter)
      ChronoUnit.DAYS.between(dateIn, dateOut)
    }
    val dtFunc2 = udf(dtFunc)

    df.filter(df("srch_co").isNotNull).withColumn("idleDays",
      dtFunc2(col("srch_ci"), col("srch_co")))
  }

  def validateHotelsDataDf(expediaData: DataFrame, hotelsData: DataFrame): DataFrame = {
    val joinedData = hotelsData.join(expediaData, hotelsData("Id") === expediaData("hotel_id"))
    val invalidData = joinedData.filter(expediaData("idleDays").>=(2).&&(expediaData("idleDays").<(30)))

    invalidData.take(5).foreach(f => println("Booking data with invalid rows: " + f))

    val validData = joinedData.except(invalidData)

    val groupedByCountry = validData.groupBy("Country").count()
    groupedByCountry.take(5).foreach(f => println("Grouped by hotel county: " + f))

    val groupedByCity = validData.groupBy("City").count()
    groupedByCity.take(5).foreach(f => println("Grouped by hotel city: " + f))

    expediaData.except(expediaData.filter(expediaData("idleDays").>=(2)
      .&&(expediaData("idleDays").<(30)))).toDF()
  }

  def storeValidExpediaData(df: DataFrame, config: Config): Unit = {
    val hdfsPath = config.getString("hdfs.hdfsPath")

    df.withColumn("year", date_format(col("srch_ci"), "yyyy"))
      .write
      .partitionBy("year")
      .format("avro")
      .save(hdfsPath)
  }

}
