package app.service.processor

import com.typesafe.config.Config
import org.apache.spark.sql.functions.{col, date_format, datediff}
import org.apache.spark.sql.{DataFrame, SaveMode}

/** Processes data in dataframe */
class DataProcessorDF {

  val MIN_DAYS = 2
  val MAX_DAYS = 30

  /** Calculates dates between check-in and check-out
   *
   * @param df expedia data
   * @return dataframe of expedia data with idle days
   */
  def calculateIdleDays(df: DataFrame) = {
    df.filter(df("srch_co").isNotNull).withColumn("idle_days",
      datediff(col("srch_co"), col("srch_ci")))
  }

  /** Validates booking data
   *
   * @param expediaData
   * @param hotelsData
   * @return dataframe of valid booking data
   */
  def validateHotelsData(expediaData: DataFrame, hotelsData: DataFrame) = {
    val invalidData = expediaData.filter(expediaData("idle_days") >= MIN_DAYS
      && expediaData("idle_days") < MAX_DAYS)

    // show info for invalid rows
    hotelsData
      .join(invalidData, hotelsData.col("id") === invalidData.col("hotel_id"))
      .show(50)

    val validData = expediaData.except(invalidData).toDF()
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
   * @param df     valid expedia data
   * @param config configuration values for the HDFS
   */
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
