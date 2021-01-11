package app.service.df

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.{col, date_format, udf}

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
    val joinedData = hotelsData
      .join(expediaData, hotelsData.col("id") === expediaData.col("hotel_id"))
    val invalidData = joinedData.filter(expediaData("idleDays") >= 2 && expediaData("idleDays") < 30)
    invalidData.take(5).foreach(f => println("Booking data with invalid rows: " + f))

    val validData = joinedData.except(invalidData)

    val groupedByCountry = validData.groupBy(col("Country")).count()
    groupedByCountry.take(5).foreach(f => println("Grouped by hotel county: " + f))

    val groupedByCity = validData.groupBy(col("City")).count()
    groupedByCity.take(5).foreach(f => println("Grouped by hotel city: " + f))

    expediaData.except(expediaData.filter(expediaData("idleDays") >= 2
      && expediaData("idleDays") < 30)).toDF()
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
