package app.partitioner

import org.apache.spark.Partitioner

/** Custom partitioner for storing data by year */
class YearPartitioner(override val numPartitions: Int) extends Partitioner {

  /** Returns the partition ID for a given key
   *
   *  @param key value to determine partition ID
   */
  def getPartition(key: Any): Int = key match {
    case year: Int => {
      if (year == 2016) 0
      else if (year == 2017) 1
      else 2
    }
  }
}
