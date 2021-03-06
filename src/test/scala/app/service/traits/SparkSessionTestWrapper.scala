package app.service.traits

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

trait SparkSessionTestWrapper {
  lazy val spark: SparkSession = {
    SparkSession.builder().
      master("local[*]")
      .appName("Spark Test")
      .getOrCreate()
  }

  val sparkContext = spark.sparkContext
}
