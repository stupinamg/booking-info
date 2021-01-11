package app.partitioner

import app.entity.ExpediaData
import org.apache.spark.Partitioner

import java.time.LocalDate
import java.time.format.DateTimeFormatter

class SrchCiPartitioner(partitions: Int) extends Partitioner {
  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[ExpediaData]
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val year = LocalDate.parse(k.srch_ci, formatter).getYear
    nonNegativeMod(year.hashCode(), numPartitions)
  }

  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }
}
