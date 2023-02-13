package counting
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object ParKmerCounting extends CountingAlgorithm {
  //TODO implementa tutto, type compreso
  override type T = this.type

  override def kmerExtraction(sequence: RDD[String], k: Broadcast[Int]): T = ???

  override def counting(kmers: T, sparkContext: SparkContext, canonical: Boolean): RDD[(String, Int)] = ???
}