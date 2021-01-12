package app.service.processor

import com.typesafe.config.Config
import org.apache.spark.sql.functions.{col, date_format, udf}
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

class DataProcessorDF {

  def calculateIdleDays(df: DataFrame) = {
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

  def validateHotelsData(expediaData: DataFrame, hotelsData: DataFrame) = {
    val invalidData = expediaData.filter(expediaData("idleDays") >= 2 && expediaData("idleDays") < 30)
    invalidData.take(5).foreach(f => println("Booking data with invalid rows: " + f))

    val joinedInvalidData = hotelsData
      .join(invalidData, hotelsData.col("id") === invalidData.col("hotel_id"))
    joinedInvalidData.take(5).foreach(f => println("Hotel data for invalid rows: " + f))

    val validData = expediaData.except(invalidData).toDF()
    val joinedValidData = validData
      .join(hotelsData, validData.col("hotel_id") === hotelsData.col("id"))

    val groupedByCountry = joinedValidData.groupBy(col("Country")).count()
    groupedByCountry.take(5).foreach(f => println("Grouped by hotel county: " + f))
    val groupedByCity = joinedValidData.groupBy(col("City")).count()
    groupedByCity.take(5).foreach(f => println("Grouped by hotel city: " + f))
    validData
  }

  def storeValidExpediaData(df: DataFrame, config: Config) = {
    val hdfsPath = config.getString("hdfs.validDataPath")

    df.withColumn("year", date_format(col("srch_ci"), "yyyy"))
      .write
      .partitionBy("year")
      .format("avro")
      .mode(SaveMode.Overwrite)
      .save(hdfsPath)
  }
}
