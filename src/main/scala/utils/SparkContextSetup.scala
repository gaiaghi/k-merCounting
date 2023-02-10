package utils
import org.apache.spark.sql.SparkSession

object SparkContextSetup {

  // number of partitions
  var PARTITIONS = 4

  def sparkSession(master: String, par: Int): SparkSession = {

    PARTITIONS = par

    val session = SparkSession.builder.appName("k-mer Counting").master(master).getOrCreate()
    session.sparkContext.setLogLevel("WARN")

    session

  }
}