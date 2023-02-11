package counting
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object ParKmerCounting extends CountingAlgorithm {
  override def counting(sequence: RDD[String], sparkContext: SparkContext, k:Broadcast[Int], canonical: String): RDD[(String, Int)] = ???
}