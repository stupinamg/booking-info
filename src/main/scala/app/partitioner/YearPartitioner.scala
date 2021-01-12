package app.partitioner

import org.apache.spark.Partitioner

class YearPartitioner(override val numPartitions: Int) extends Partitioner {
  // returns the partition ID for a given key
  def getPartition(key: Any): Int = key match {
    case year: Int => {
      if (year == 2016) 0
      else if (year == 2017) 1
      else 2
    }
  }
}
