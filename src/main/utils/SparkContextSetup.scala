package utils
import org.apache.spark.sql.SparkSession

object SparkContextSetup {
  var DEF_PAR = 1 // number of partitions

  private def _sparkSession(master: String): SparkSession = {
    var builder = SparkSession.builder.appName("k-mer Counting")

    if (master != "default") {
      builder = builder.master(master)
    }
    builder.getOrCreate()
  }

  def sparkSession(master: String, par: Int): SparkSession = {
    val session = _sparkSession(master)

    DEF_PAR = par
    session.sparkContext.setLogLevel("WARN")

    session
  }
}